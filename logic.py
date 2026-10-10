"""Pure validation, scheduling and reports for the Wait block.

Nothing here sleeps, starts a thread, opens a socket or reads a clock on its own:
callers pass the monotonic and wall-clock times, so every rule is testable.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Mapping
from datetime import datetime, timezone
import heapq
import json
import math

UTC = timezone.utc
DEFAULTS = {"mode": "duration", "seconds": 5, "until": "", "until_path": "", "report_cancelled": True,
            "max_pending": 1000, "max_message_bytes": 262144, "max_pending_bytes": 67108864}
RESERVED = {"execution", "runtime_path", "runtime_path_label"}
MODES = ("duration", "until")
ACTIONS = ("flush", "cancel")

# One Shot executes an activation synchronously; the managed package host stops an
# activation after 300 s, so the synchronous wait keeps a clear margin below it.
ONE_SHOT_MAX_SEC = 240
UNTIL_HORIZON_SEC = 366 * 86400
MAX_COMMAND_BYTES = 1024
MAX_POINTER_CHARS = 512
MAX_INSTANT_CHARS = 64

# Framework listener mailboxes accept JSON snapshots up to 1 MiB (ASCII-escaped).
# A message must fit in a command and later in a result with a little metadata.
MAILBOX_LIMIT = 1_048_576
MAILBOX_VALUE_LIMIT = 1_000_000

# The framework's listener result queue holds 128 entries and is drained by the
# worker between activations. Releases are paced so it can never overflow:
# at most EMIT_LIMIT results may be unconfirmed; drains are confirmed exactly by
# activations, or assumed at a conservative rate once no activation arrived for
# QUIET_SEC (the worker then drains its queue every 0.1 s).
EMIT_LIMIT = 48
QUIET_SEC = 1.0
IDLE_DRAIN_PER_SEC = 24.0
MAX_NOTES = 20
STATUS_INTERVAL_SEC = 0.5

DETAILS = {
    "cancel": "Dropped by a cancel command before its release.",
    "stop": "The run stopped before the release.",
    "max_pending": "Refused: {limit} messages are already waiting (max_pending).",
    "max_pending_bytes": "Refused: waiting messages would exceed {limit} bytes (max_pending_bytes).",
}


class WaitError(ValueError):
    """Invalid settings or command. Details are English and never echo message contents."""


class Rejected(Exception):
    """A message that cannot be scheduled; it is reported, never silently dropped."""

    def __init__(self, reason: str, detail: str) -> None:
        super().__init__(detail)
        self.reason = reason
        self.detail = detail


def thaw(value):
    """Preparation and listener snapshots are recursively immutable; return plain values."""
    if isinstance(value, Mapping):
        return {key: thaw(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [thaw(item) for item in value]
    return value


def _number(value) -> bool:
    """Accept finite JSON numbers, never booleans or numeric strings."""
    return type(value) in (int, float) and math.isfinite(value)


def configuration(raw, now: datetime | None = None) -> dict:
    """Validate every setting before any message is accepted; unknown keys are refused."""
    raw = thaw(raw)
    if not isinstance(raw, dict) or set(raw) - set(DEFAULTS) - RESERVED:
        raise WaitError("Unknown wait setting.")
    config = {**DEFAULTS, **{key: value for key, value in raw.items() if key in DEFAULTS}}
    if config["mode"] not in MODES:
        raise WaitError("mode must be duration or until.")
    if not _number(config["seconds"]) or not 0.1 <= config["seconds"] <= 86400:
        raise WaitError("seconds must be a number between 0.1 and 86400.")
    for key, low, high in (("max_pending", 1, 10000), ("max_message_bytes", 1, 1_000_000),
                           ("max_pending_bytes", 1_048_576, 1_073_741_824)):
        if type(config[key]) is not int or not low <= config[key] <= high:
            raise WaitError(f"{key} must be an integer between {low} and {high}.")
    if type(config["report_cancelled"]) is not bool:
        raise WaitError("report_cancelled must be true or false.")
    if not isinstance(config["until"], str) or len(config["until"]) > MAX_INSTANT_CHARS:
        raise WaitError("until must be an ISO 8601 instant text.")
    if not isinstance(config["until_path"], str) or len(config["until_path"]) > MAX_POINTER_CHARS:
        raise WaitError("until_path must be a JSON Pointer text.")
    config["until"] = config["until"].strip()
    config["until_path"] = config["until_path"].strip()
    if config["until_path"]:
        pointer_parts(config["until_path"])
    if config["until"]:
        instant(config["until"], now or datetime.now(UTC))
    if config["mode"] == "until" and not (config["until"] or config["until_path"]):
        raise WaitError("until mode needs an until instant or an until_path.")
    return config


def instant(text, now: datetime) -> datetime:
    """Parse an ISO 8601 instant with an explicit offset, within the accepted horizon."""
    if not isinstance(text, str) or not 1 <= len(text.strip()) <= MAX_INSTANT_CHARS:
        raise WaitError("Expected an ISO 8601 instant such as 2026-10-10T14:30:00Z.")
    try:
        parsed = datetime.fromisoformat(text.strip())
    except ValueError:
        raise WaitError("Expected an ISO 8601 instant such as 2026-10-10T14:30:00Z.") from None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise WaitError("The instant needs an explicit UTC offset or Z, for example 2026-10-10T14:30:00Z.")
    parsed = parsed.astimezone(UTC)
    if (parsed - now).total_seconds() > UNTIL_HORIZON_SEC:
        raise WaitError("The instant is more than 366 days ahead.")
    return parsed


def iso(value: float | datetime | None) -> str | None:
    """Format a wall-clock time as UTC ISO 8601 with milliseconds."""
    if value is None:
        return None
    moment = value if isinstance(value, datetime) else datetime.fromtimestamp(value, UTC)
    return moment.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def pointer_parts(path) -> list[str]:
    """Decode an RFC 6901 JSON Pointer; malformed escapes never address another field."""
    if not isinstance(path, str) or len(path) > MAX_POINTER_CHARS or not path.startswith("/"):
        raise WaitError("until_path must be a JSON Pointer starting with /, for example /release_at.")
    parts = []
    for part in path.split("/")[1:]:
        for index, char in enumerate(part):
            if char == "~" and (index + 1 == len(part) or part[index + 1] not in "01"):
                raise WaitError("until_path contains an invalid ~ escape.")
        parts.append(part.replace("~1", "/").replace("~0", "~"))
    if len(parts) > 32:
        raise WaitError("until_path is deeper than 32 levels.")
    return parts


def at_pointer(document, parts):
    """Return the addressed value, or None when it is missing (null is not an instant either)."""
    for part in parts:
        if isinstance(document, dict):
            document = document.get(part)
        elif (isinstance(document, list) and part.isascii() and part.isdigit()
              and str(int(part)) == part and int(part) < len(document)):
            document = document[int(part)]
        else:
            return None
    return document


def message_text(value) -> tuple[str, bool]:
    """Messages travel as text; native One Shot values are serialized as JSON."""
    if isinstance(value, str):
        return value, False
    try:
        return json.dumps(value, ensure_ascii=False, allow_nan=False), True
    except (TypeError, ValueError, RecursionError):
        return str(value), False


def incoming(value, content_type, config: dict, *, received_wall: float, ident: str, mailbox: bool) -> dict:
    """Validate one message and resolve its due time; raise Rejected when it cannot wait."""
    text, serialized = message_text(value)
    content_type = "application/json" if serialized else (str(content_type or "") or "text/plain")[:128]
    size = len(text.encode("utf-8"))
    item = {"id": ident, "value": text, "content_type": content_type, "bytes": size,
            "received_wall": received_wall, "received_at": iso(received_wall), "due_at": None}
    if size > config["max_message_bytes"]:
        raise Rejected("too_large", f"The message exceeds max_message_bytes ({config['max_message_bytes']}).")
    if mailbox and len(json.dumps(text, ensure_ascii=True)) > MAILBOX_VALUE_LIMIT:
        raise Rejected("too_large", "Once JSON-escaped, the message exceeds the Active Runtime listener mailbox.")
    now = datetime.fromtimestamp(received_wall, UTC)
    if config["mode"] == "duration":
        item["delay"] = float(config["seconds"])
        item["due_at"] = iso(received_wall + item["delay"])
        return item
    if config["until_path"]:
        try:
            document = json.loads(text)
        except (ValueError, RecursionError):
            raise Rejected("invalid_until", "until_path needs a JSON message; this message is not JSON.") from None
        found = at_pointer(document, pointer_parts(config["until_path"]))
        if found is None:
            raise Rejected("invalid_until", "No value at until_path in this message.")
        try:
            moment = instant(found, now)
        except WaitError as exc:
            raise Rejected("invalid_until", f"Invalid instant at until_path: {exc}") from None
    else:
        moment = instant(config["until"], now)
    item["epoch"] = moment.timestamp()
    item["due_at"] = iso(moment)
    return item


def delay_seconds(item: dict, now_wall: float) -> float:
    """Remaining wait for a synchronous One Shot activation; past instants give 0."""
    if "delay" in item:
        return max(0.0, item["delay"] - max(0.0, now_wall - item["received_wall"]))
    return max(0.0, item["epoch"] - now_wall)


def command(value) -> str:
    """Accept exactly {"action":"flush"} or {"action":"cancel"}, as an object or strict JSON text."""
    if isinstance(value, str):
        if len(value.encode("utf-8")) > MAX_COMMAND_BYTES:
            raise WaitError("Commands are limited to 1 KiB.")

        def unique(pairs):
            result = {}
            for key, item in pairs:
                if key in result:
                    raise WaitError("Duplicate command property.")
                result[key] = item
            return result
        try:
            value = json.loads(value, object_pairs_hook=unique)
        except (ValueError, RecursionError) as exc:
            if isinstance(exc, WaitError):
                raise
            raise WaitError('Commands are JSON: {"action":"flush"} or {"action":"cancel"}.') from None
    value = thaw(value)
    if not isinstance(value, dict) or set(value) != {"action"} or value["action"] not in ACTIONS:
        raise WaitError('Commands are exactly {"action":"flush"} or {"action":"cancel"}.')
    return value["action"]


def envelope(item: dict, reason: str, detail: str, dropped_wall: float, *, include_message: bool = True) -> dict:
    """Describe a message that will never leave on out; it keeps the original text when possible."""
    report = {"id": item["id"], "reason": reason, "detail": detail, "received_at": item.get("received_at"),
              "due_at": item.get("due_at"), "dropped_at": iso(dropped_wall),
              "content_type": item["content_type"], "message_bytes": item["bytes"]}
    if include_message:
        report["message"] = item["value"]
    else:
        report["message_omitted"] = True
    return report


def snapshot_size(payload) -> int:
    """Measure a value the way the framework mailbox does (ASCII JSON)."""
    return len(json.dumps(payload, ensure_ascii=True, allow_nan=False))


class Scheduler:
    """State of one listener generation: bounded queue, ordered releases and paced results.

    Admission counts messages that still have to leave on out. Reports of refused or
    cancelled messages are bounded separately; beyond that bound they are counted
    as unreported instead of growing memory.
    """

    def __init__(self, config: dict, generation: str, now: float) -> None:
        self.config = config
        self.generation = generation
        self.heap: list = []
        self.order = 0
        self.outbox: deque = deque()
        self.out_count = 0
        self.held_bytes = 0
        self.report_count = 0
        self.report_bytes = 0
        self.counters = {"admitted": 0, "released": 0, "cancelled": 0, "rejected": 0, "unreported": 0}
        self.notes: list[str] = []
        self.dropped_notes = 0
        self.emitted = 0
        self.acked = 0
        self.outstanding = 0.0
        self.last_activity = now
        self.last_tick = now
        self.last_status = now
        self.reported_pending = 0

    # --- result pacing ---------------------------------------------------------------
    def activity(self, now: float, ack) -> None:
        """An activation reached the listener: the worker was busy and reports what it drained."""
        self.last_activity = self.last_tick = now
        if isinstance(ack, Mapping) and ack.get("generation") == self.generation:
            confirmed = ack.get("emitted")
            if type(confirmed) is int and self.acked <= confirmed <= self.emitted:
                self.acked = confirmed
        self.outstanding = float(self.emitted - self.acked)

    def refresh(self, now: float) -> None:
        """Assume a conservative drain only after QUIET_SEC without any activation."""
        start = max(self.last_tick, self.last_activity + QUIET_SEC)
        if now > start and self.outstanding > 0:
            self.outstanding = max(0.0, self.outstanding - (now - start) * IDLE_DRAIN_PER_SEC)
        self.last_tick = max(self.last_tick, now)

    def can_emit(self) -> bool:
        return self.outstanding <= EMIT_LIMIT - 1

    def emitted_one(self) -> None:
        self.emitted += 1
        self.outstanding += 1.0

    # --- notes (English traces, never message contents) --------------------------------
    def note(self, line: str) -> None:
        if len(self.notes) < MAX_NOTES:
            self.notes.append(line)
        else:
            self.dropped_notes += 1

    def take_notes(self) -> list[str]:
        notes = list(self.notes)
        if self.dropped_notes:
            notes.append(f"[wait] {self.dropped_notes} more trace line(s) coalesced.")
        self.notes, self.dropped_notes = [], 0
        return notes

    # --- queue -------------------------------------------------------------------------
    def pending(self) -> int:
        return len(self.heap) + self.out_count

    def admit(self, item: dict, now_mono: float, now_wall: float) -> bool:
        """Queue one validated message, or report its refusal; ties keep arrival order."""
        if self.pending() >= self.config["max_pending"]:
            self.refuse(item, "max_pending", DETAILS["max_pending"].format(limit=self.config["max_pending"]), now_wall)
            return False
        if self.held_bytes + item["bytes"] > self.config["max_pending_bytes"]:
            self.refuse(item, "max_pending_bytes",
                        DETAILS["max_pending_bytes"].format(limit=self.config["max_pending_bytes"]), now_wall)
            return False
        if "delay" in item:
            # Transit from execute_runtime is short; bound it so a wall-clock jump cannot shift the wait.
            elapsed = min(max(0.0, now_wall - item["received_wall"]), 2.0)
            deadline = now_mono - elapsed + item["delay"]
        else:
            deadline = now_mono + (item["epoch"] - now_wall)
        # The framework keeps listener and execute_runtime settings identical, so one heap holds one mode.
        # Every duration message waits the same time: due order is arrival order. Until messages sort
        # on their exact instant, so equal instants keep arrival order. Releases follow the monotonic
        # deadline, so a later wall-clock change cannot move a wait already admitted.
        key = float(item["epoch"]) if "epoch" in item else 0.0
        heapq.heappush(self.heap, (key, self.order, deadline, item))
        self.order += 1
        self.held_bytes += item["bytes"]
        self.counters["admitted"] += 1
        return True

    def refuse(self, item: dict, reason: str, detail: str, now_wall: float) -> None:
        self.counters["rejected"] += 1
        self.note(f"[wait] Message {item['id']} refused ({reason}).")
        self.report(item, reason, detail, now_wall)

    def report(self, item: dict, reason: str, detail: str, now_wall: float) -> None:
        """Queue a cancelled-port report within its own bound, otherwise count it."""
        if not self.config["report_cancelled"]:
            return
        if (self.report_count >= self.config["max_pending"]
                or self.report_bytes + item["bytes"] > self.config["max_pending_bytes"]):
            self.counters["unreported"] += 1
            return
        self.outbox.append(("cancelled", envelope(item, reason, detail, now_wall), item["bytes"]))
        self.report_count += 1
        self.report_bytes += item["bytes"]

    def release_due(self, now_mono: float) -> int:
        """Move due messages to the outbox in due order (ties in arrival order)."""
        moved = 0
        while self.heap and self.heap[0][2] <= now_mono:
            item = heapq.heappop(self.heap)[3]
            self.outbox.append(("out", item, item["bytes"]))
            self.out_count += 1
            moved += 1
        return moved

    def flush(self) -> int:
        """Release everything pending now, keeping due-time order."""
        count = len(self.heap)
        self.release_due(math.inf)
        self.note(f"[wait] Flush: {count} pending message(s) released now.")
        return count

    def cancel(self, now_wall: float, reason: str = "cancel") -> int:
        """Drop every message not yet published; queued reports are kept."""
        dropped = [entry[1] for entry in self.outbox if entry[0] == "out"]
        self.outbox = deque(entry for entry in self.outbox if entry[0] != "out")
        while self.heap:
            dropped.append(heapq.heappop(self.heap)[3])
        self.out_count = 0
        self.held_bytes = 0
        self.counters["cancelled"] += len(dropped)
        for item in dropped:
            self.report(item, reason, DETAILS[reason], now_wall)
        self.note(f"[wait] Cancel: {len(dropped)} pending message(s) dropped.")
        return len(dropped)

    def status_due(self) -> bool:
        """Traces to publish, or a pending count that changed since the last published state."""
        return bool(self.notes or self.dropped_notes or self.pending() != self.reported_pending)

    def next_entry(self, now_mono: float):
        """Return the next result to publish, or a rate-limited status entry."""
        if self.outbox:
            return self.outbox[0]
        if self.status_due() and now_mono - self.last_status >= STATUS_INTERVAL_SEC:
            return ("status", None, 0)
        return None

    def pop_entry(self, entry, now_mono: float) -> None:
        """Account for a published entry; status entries are not in the outbox."""
        kind, payload, size = entry
        if kind == "status":
            self.last_status = now_mono
            return
        self.outbox.popleft()
        if kind == "out":
            self.out_count -= 1
            self.held_bytes -= size
            self.counters["released"] += 1
        else:
            self.report_count -= 1
            self.report_bytes -= size

    def wait_timeout(self, now_mono: float) -> float:
        """How long the listener may block on its mailbox before it has work to do."""
        if self.outbox:
            return 0.0 if self.can_emit() else 0.05
        if self.status_due():
            return max(0.0, min(0.1, self.last_status + STATUS_INTERVAL_SEC - now_mono))
        if self.heap:
            return max(0.0, min(0.1, self.heap[0][2] - now_mono))
        return 0.1

    def state(self, now_mono: float, now_wall: float) -> dict:
        """Counters and schedule for metadata; never message contents. Marks the count as published."""
        next_due = iso(now_wall + max(0.0, self.heap[0][2] - now_mono)) if self.heap else None
        self.reported_pending = self.pending()
        return {"live": True, "pending": self.pending(), "pending_bytes": self.held_bytes,
                "reports_queued": self.report_count, "next_due_at": next_due, **self.counters}

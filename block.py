"""Delay each message by a duration or until an instant without blocking the rest of the graph."""

from collections.abc import Mapping
from dataclasses import asdict
import json
import math
import time
import uuid

from bloxsmith_app.block_api import (APPLICATION_JSON, BlockDefinition, BlockRuntimeOutput,
                                     BlockRuntimePreparation, BlockRuntimeResult)
from . import logic, ui


# FB1 - Release each message unchanged on out after a duration from 0.1 to 86400 seconds.
# FB2 - Release at an ISO 8601 instant from the settings or from a JSON Pointer in each message; past instants release at once.
# FB3 - Release in due-time order, ties in arrival order, pacing results so the listener queue never overflows.
# FB4 - Bound admission by max_pending and bytes; refused or invalid messages are reported on cancelled, never dropped silently.
# FB5 - Apply strict {"action":"flush"} and {"action":"cancel"} commands from command_in to pending messages.
# FB6 - Handle Stop: cancellable One Shot waits, cooperative listener shutdown and per-message traces.
# FB7 - Run in One Shot (bounded synchronous wait) and Active Runtime (Run listener) for every package origin.
# FB8 - Own translated responsive settings with mode-specific fields, draft/Apply and release-scoped assets.
IN_PORT, COMMAND_PORT, OUT_PORT, CANCELLED_PORT = 1, 2, 1, 2
MAX_EVENTS_PER_ACTIVATION = 16


def _port_name(ports, port_id: int, fallback: str) -> str:
    """Ports are addressed by stable id; a renamed port keeps working."""
    return next((str(port.name) for port in ports if int(port.id) == port_id), fallback)


def _received_wall(context, port_id: int) -> float:
    """Use the arrival time stamped by the worker when it is plausible, otherwise now."""
    now = time.time()
    attribute = context.input_attribute(_port_name(context.input_ports, port_id, "in"), str(port_id))
    stamp = getattr(attribute, "updated_at", None) if attribute is not None else None
    if type(stamp) in (int, float) and math.isfinite(stamp) and now - 30 <= stamp <= now + 1:
        return min(float(stamp), now)
    return now


def _ack(previous) -> dict | None:
    """Forward the last listener result the worker has published, as a pacing confirmation."""
    raw = previous.get("wait_ack") if isinstance(previous, Mapping) else None
    if isinstance(raw, Mapping) and isinstance(raw.get("generation"), str) and type(raw.get("emitted")) is int:
        return {"generation": raw["generation"], "emitted": raw["emitted"]}
    return None


class WaitBlock(BlockDefinition):
    """Messages wait in a Run-scoped listener; One Shot waits inside the activation."""

    kind = "wait"

    def ui_assets(self, surface="modal"):
        """Serve only the static manifest declarations, for every package origin."""
        return list(self.model.get("ui_assets", {}).get(surface, []))

    def prepare_runtime(self, context):
        """Validate settings purely and declare the Active Runtime listener."""
        logic.configuration(context.config)
        return BlockRuntimePreparation(listen_on_run=context.runtime_mode == "zeromq_active")

    # --- activations ----------------------------------------------------------------------
    def execute_runtime(self, context):
        """Forward messages and commands to the listener, or wait synchronously in One Shot."""
        try:
            config = logic.configuration(context.config)
        except logic.WaitError as exc:
            return self._failure(str(exc))
        if context.runtime_mode == "zeromq_active":
            return self._forward(context, config)
        return self._one_shot(context, config)

    def _failure(self, detail: str, message: str = "") -> BlockRuntimeResult:
        """An explicit failure for invalid settings or an unavailable framework bridge."""
        text = message or self.translate("block.wait.error", {"detail": detail}, fallback="Wait failed: {detail}")
        return BlockRuntimeResult(status="failed", outputs=[], error=text, exit_code=1, last_message=text,
                                  logs=[f"[wait-error] {detail}"])

    def _report_output(self, context, item: dict, reason: str, detail: str) -> BlockRuntimeOutput:
        report = logic.envelope(item, reason, detail, time.time())
        return BlockRuntimeOutput(port_id=CANCELLED_PORT, port_name=_port_name(context.output_ports, CANCELLED_PORT, "cancelled"),
                                  value=json.dumps(report, ensure_ascii=False), content_type=APPLICATION_JSON)

    def _rejection(self, context, config, item_value, content_type, received_wall, ident, rejected) -> tuple[list, str]:
        """Report a message that cannot be scheduled; it never reaches out."""
        log = f"[wait] Message {ident} refused ({rejected.reason}): {rejected.detail}"
        outputs = []
        if config["report_cancelled"]:
            text, _ = logic.message_text(item_value)
            item = {"id": ident, "value": text, "content_type": str(content_type or "text/plain")[:128],
                    "bytes": len(text.encode("utf-8")), "received_at": logic.iso(received_wall), "due_at": None}
            keep = rejected.reason != "too_large"
            report = logic.envelope(item, rejected.reason, rejected.detail, time.time(), include_message=keep)
            outputs.append(BlockRuntimeOutput(port_id=CANCELLED_PORT,
                           port_name=_port_name(context.output_ports, CANCELLED_PORT, "cancelled"),
                           value=json.dumps(report, ensure_ascii=False), content_type=APPLICATION_JSON))
        return outputs, log

    def _forward(self, context, config):
        """Active Runtime: validate and forward quickly; the listener owns timing and order."""
        known = {int(port.id) for port in context.input_ports}
        ack = _ack(context.previous_result)
        commands, outputs, logs = [], [], []
        events = list(context.input_events)[:MAX_EVENTS_PER_ACTIVATION]
        if len(context.input_events) > MAX_EVENTS_PER_ACTIVATION:
            logs.append(f"[wait] {len(context.input_events) - MAX_EVENTS_PER_ACTIVATION} extra event(s) in one activation ignored.")
        for event in events:
            port_id = int(event.input_port_id) if int(event.input_port_id) in known else None
            value = event.value
            if port_id == IN_PORT:
                if value in (None, ""):
                    logs.append("[wait] Empty message ignored.")
                    continue
                received_wall, ident = _received_wall(context, IN_PORT), uuid.uuid4().hex
                try:
                    item = logic.incoming(value, event.content_type, config, received_wall=received_wall,
                                          ident=ident, mailbox=True)
                except logic.Rejected as rejected:
                    rejected_outputs, log = self._rejection(context, config, value, event.content_type,
                                                            received_wall, ident, rejected)
                    outputs.extend(rejected_outputs)
                    logs.append(log)
                    continue
                commands.append({"op": "message", "item": item})
                logs.append(f"[wait] Message {ident} queued ({item['bytes']} bytes), due {item['due_at']}.")
            elif port_id == COMMAND_PORT:
                try:
                    action = logic.command(value)
                except logic.WaitError as exc:
                    logs.append(f"[wait] Command refused: {exc}")
                    continue
                commands.append({"op": action})
                logs.append(f"[wait] Command {action} forwarded.")
            else:
                logs.append("[wait] Event on an unknown input ignored.")
        sender = context.services.get("runtime_listener")
        if sender is None:
            return self._failure("The Run listener bridge is unavailable; nothing was queued.",
                                 self.translate("block.wait.listener_unavailable",
                                                fallback="The wait listener is unavailable; nothing was queued. Stop and Run again."))
        try:
            for item in commands or [{"op": "ping"}]:
                sender.send({**item, "ack": ack})
        except Exception:
            return self._failure("The Run listener refused the command; it was not queued.",
                                 self.translate("block.wait.listener_refused",
                                                fallback="The wait listener refused the message (stopped or full); it was not queued."))
        previous = context.previous_result if isinstance(context.previous_result, Mapping) else {}
        metadata = {"wait_activation": {"forwarded": len(commands), "refused": len(outputs)}}
        if isinstance(previous.get(self.kind), Mapping):
            # Keep the listener's last published state visible between its own results.
            metadata[self.kind] = dict(previous[self.kind])
        summary = f"{len(commands)} item(s) forwarded to the wait queue." if commands else "Nothing new to queue."
        return BlockRuntimeResult(status="success", outputs=outputs, logs=logs, last_message=summary,
                                  metadata=metadata)

    def _one_shot(self, context, config):
        """One Shot: one fresh message per activation, waited synchronously and cancellably."""
        cancel = context.services.get("cancel_requested") or (lambda: False)
        in_name = _port_name(context.input_ports, IN_PORT, "in")
        command_name = _port_name(context.input_ports, COMMAND_PORT, "command_in")
        logs = []
        if context.has_input_value(command_name, str(COMMAND_PORT)):
            logs.append("[wait] One Shot keeps nothing pending between activations; command_in has no effect.")
        event = next((item for item in reversed(context.input_events) if int(item.input_port_id) == IN_PORT), None)
        value = context.input_value(in_name, str(IN_PORT), default="")
        content_type = event.content_type if event is not None else context.input_content_type(in_name, str(IN_PORT))
        if value in (None, ""):
            return BlockRuntimeResult(status="skipped", outputs=[], logs=[*logs, "[wait] No message on in."],
                                      last_message="No message to wait for.")
        received_wall, ident = time.time(), uuid.uuid4().hex
        try:
            item = logic.incoming(value, content_type, config, received_wall=received_wall, ident=ident, mailbox=False)
        except logic.Rejected as rejected:
            outputs, log = self._rejection(context, config, value, content_type, received_wall, ident, rejected)
            return BlockRuntimeResult(status="success", outputs=outputs, logs=[*logs, log],
                                      last_message=f"Message refused: {rejected.detail}",
                                      metadata={self.kind: {"live": False, "refused": rejected.reason}})
        delay = logic.delay_seconds(item, time.time())
        if delay > logic.ONE_SHOT_MAX_SEC:
            message = self.translate("block.wait.one_shot_limit",
                                     {"max": logic.ONE_SHOT_MAX_SEC, "delay": f"{delay:.1f}"},
                                     fallback="One Shot waits at most {max} s; this message would wait {delay} s. Use Active Runtime for longer waits.")
            return self._failure(f"One Shot wait of {delay:.1f} s exceeds {logic.ONE_SHOT_MAX_SEC} s.", message)
        deadline = time.monotonic() + delay
        while True:
            if cancel():
                outputs = [self._report_output(context, item, "stop", logic.DETAILS["stop"])] if config["report_cancelled"] else []
                return BlockRuntimeResult(status="skipped", outputs=outputs,
                                          logs=[*logs, f"[wait] Stop requested; message {ident} was not released."],
                                          last_message="Stopped before the release.",
                                          metadata={self.kind: {"live": False, "cancelled": 1}})
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            time.sleep(min(0.1, remaining))
        out = BlockRuntimeOutput(port_id=OUT_PORT, port_name=_port_name(context.output_ports, OUT_PORT, "out"),
                                 value=item["value"], content_type=item["content_type"])
        return BlockRuntimeResult(status="success", outputs=[out], content_type=item["content_type"],
                                  logs=[*logs, f"[wait] Released {ident} after {delay:.2f} s ({item['bytes']} bytes)."],
                                  last_message=f"Released after {delay:.2f} s.",
                                  metadata={self.kind: {"live": False, "waited_sec": round(delay, 3), "released": 1}})

    # --- Active Runtime listener -------------------------------------------------------------
    def listen_runtime(self, context):
        """Own the queue for this Run; state stays local to this hook, never on the block."""
        config = logic.configuration(context.config)
        names = {OUT_PORT: _port_name(context.output_ports, OUT_PORT, "out"),
                 CANCELLED_PORT: _port_name(context.output_ports, CANCELLED_PORT, "cancelled")}
        scheduler = logic.Scheduler(config, uuid.uuid4().hex, time.monotonic())
        while not context.stop_requested():
            received = context.receive_command(timeout_sec=scheduler.wait_timeout(time.monotonic()))
            handled = 0
            while received is not None:
                self._apply(scheduler, received.payload)
                handled += 1
                if handled >= 128 or context.stop_requested():
                    break
                received = context.receive_command(timeout_sec=0)
            now = time.monotonic()
            scheduler.refresh(now)
            scheduler.release_due(now)
            for _ in range(16):
                if context.stop_requested() or not scheduler.can_emit():
                    break
                now = time.monotonic()
                entry = scheduler.next_entry(now)
                if entry is None:
                    break
                scheduler.pop_entry(entry, now)
                result = self._listener_result(scheduler, names, entry, now, time.time())
                try:
                    context.emit_result(result)
                except Exception:
                    # After Stop the framework refuses results; pending messages end with the Run.
                    if context.stop_requested():
                        return
                    raise
                scheduler.emitted_one()

    def _apply(self, scheduler, payload) -> None:
        """Apply one forwarded command in arrival order; malformed internal commands are traced."""
        now_mono, now_wall = time.monotonic(), time.time()
        if not isinstance(payload, dict):
            scheduler.note("[wait] Malformed listener command ignored.")
            return
        scheduler.activity(now_mono, payload.get("ack"))
        operation = payload.get("op")
        try:
            if operation == "message":
                item = payload.get("item")
                if not isinstance(item, dict) or not isinstance(item.get("value"), str) or not isinstance(item.get("id"), str):
                    raise ValueError("malformed item")
                scheduler.admit(item, now_mono, now_wall)
            elif operation == "flush":
                scheduler.flush()
            elif operation == "cancel":
                scheduler.cancel(now_wall)
            elif operation != "ping":
                raise ValueError("unknown operation")
        except (ValueError, TypeError, KeyError):
            scheduler.note("[wait] Malformed listener command ignored.")

    def _listener_result(self, scheduler, names, entry, now_mono, now_wall) -> BlockRuntimeResult:
        """One result per published message; the framework publishes one value per port and result."""
        kind, payload, _size = entry
        sequence = scheduler.emitted + 1
        logs, outputs = scheduler.take_notes(), []
        state = scheduler.state(now_mono, now_wall)
        if kind == "out":
            outputs.append(BlockRuntimeOutput(port_id=OUT_PORT, port_name=names[OUT_PORT], value=payload["value"],
                                              content_type=payload["content_type"]))
            logs.append(f"[wait] Released {payload['id']} ({payload['bytes']} bytes), due {payload['due_at']}.")
            summary = f"Released 1 message; {state['pending']} pending."
        elif kind == "cancelled":
            outputs.append(BlockRuntimeOutput(port_id=CANCELLED_PORT, port_name=names[CANCELLED_PORT],
                                              value=json.dumps(payload, ensure_ascii=False), content_type=APPLICATION_JSON))
            logs.append(f"[wait] Reported {payload['id']} on cancelled ({payload['reason']}).")
            summary = f"Reported 1 message as {payload['reason']}; {state['pending']} pending."
        else:
            summary = f"{state['pending']} message(s) pending."
        metadata = {self.kind: state, "wait_ack": {"generation": scheduler.generation, "emitted": sequence}}
        result = BlockRuntimeResult(status="success", outputs=outputs, logs=logs, last_message=summary, metadata=metadata)
        if logic.snapshot_size(asdict(result)) > logic.MAILBOX_LIMIT - 4096:
            # Keep the result within the mailbox: a report drops its copy, traces are shortened.
            if kind == "cancelled":
                lighter = {key: value for key, value in payload.items() if key != "message"}
                lighter["message_omitted"] = True
                outputs[0].value = json.dumps(lighter, ensure_ascii=False)
            result.logs = logs[-1:]
        return result

    # --- UI ------------------------------------------------------------------------------------
    def handle_ui_action(self, *, node, action, values, payload=None):
        """Validate settings before persisting; Apply never sends a Run command."""
        if action in {"save_settings", "modal_update_fields", "inspector_update_fields"}:
            patch = (values or {}).get("node_patch") or {}
            if "config" in patch:
                try:
                    logic.configuration({**self.default_config(), **(node.get("config") or {}), **patch["config"]})
                except (ValueError, TypeError) as exc:
                    return {"error": self.translate("block.wait.error", {"detail": str(exc)}, fallback="Wait failed: {detail}")}
        result = super().handle_ui_action(node=node, action="modal_update_fields" if action == "save_settings" else action,
                                          values=values, payload=payload)
        return ui.replace_config(result, node)

    def render_modal(self, *, node, payload=None):
        """Show only the fields of the selected mode, plus command help."""
        return ui.modal(self, node, payload)

    def render_inspector_panel(self, *, node, payload=None):
        """Offer the same settings in the narrow inspector, with the standard Ports tab."""
        return ui.inspector(self, node, payload)

    def render_node_card(self, *, node, payload=None):
        """Summarize the delay rule without exposing message contents."""
        return ui.card(self, node)

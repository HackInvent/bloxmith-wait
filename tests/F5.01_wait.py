"""FB1-FB8 contract: settings, instants, ordering, bounds, commands, pacing, Stop and surfaces.

Pure scheduling uses injected clocks; the hooks run with real short waits and
framework context objects, never a server, a provider or a credential.
"""

from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import queue
import sys
import threading
import time
from types import MappingProxyType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.wait import logic
from blocs.wait.block import WaitBlock
from bloxsmith_app.block_api import BlockRuntimeContext, BlockRuntimeResult, RuntimeListenerCommand
from bloxsmith_app.block_runtime import BlockInputEvent

UTC = timezone.utc
NOW = datetime(2026, 10, 10, 12, 0, tzinfo=UTC)
SECRET_TEXT = "private-payload-never-in-logs"
INPUTS = (SimpleNamespace(id=1, name="in"), SimpleNamespace(id=2, name="command_in"))
OUTPUTS = (SimpleNamespace(id=1, name="out"), SimpleNamespace(id=2, name="cancelled"))


def refused(call, error=logic.WaitError):
    """An invalid value must be refused explicitly, never coerced."""
    try:
        call()
    except error as exc:
        return exc
    raise AssertionError("Invalid value accepted")


def config(**overrides):
    return logic.configuration({**logic.DEFAULTS, **overrides}, NOW)


def item(ident, *, delay=None, epoch=None, size=10, received=1000.0):
    """A validated message as execute_runtime forwards it."""
    entry = {"id": ident, "value": ident, "content_type": "text/plain", "bytes": size,
             "received_wall": received, "received_at": logic.iso(received), "due_at": None}
    if delay is not None:
        entry["delay"] = delay
    else:
        entry["epoch"] = epoch
    return entry


def settings_and_instants():
    """FB1/FB2 - bounds, explicit offsets, JSON Pointers and unknown keys."""
    # - FB1 - seconds bounds; FB2 - until instants and until_path pointers.
    assert config()["mode"] == "duration" and config(seconds=0.1)["seconds"] == 0.1
    assert logic.configuration(MappingProxyType({"mode": "until", "until_path": "/a~1b/~0c"}), NOW)["until_path"] == "/a~1b/~0c"
    assert config(execution={"inhibited": False}, runtime_path=[], runtime_path_label="x")["max_pending"] == 1000
    for bad in ({"mode": "later"}, {"seconds": 0.05}, {"seconds": 86400.5}, {"seconds": True}, {"seconds": "5"},
                {"seconds": float("nan")}, {"max_pending": 0}, {"max_pending": 10001}, {"max_pending": 5.0},
                {"max_message_bytes": 1_000_001}, {"max_pending_bytes": 1024}, {"report_cancelled": "yes"},
                {"until": "2026-10-10T12:00:00"}, {"until": "tomorrow"}, {"until": "2028-10-10T12:00:00Z"},
                {"until_path": "release_at"}, {"until_path": "/bad~2"}, {"mode": "until"}, {"other": 1}):
        refused(lambda raw=bad: logic.configuration({**logic.DEFAULTS, **raw}, NOW))
    assert logic.instant("2026-10-10T14:30:00Z", NOW) == datetime(2026, 10, 10, 14, 30, tzinfo=UTC)
    assert logic.instant("2026-10-10T16:30:00.250+02:00", NOW) == datetime(2026, 10, 10, 14, 30, 0, 250000, tzinfo=UTC)
    assert logic.instant("2020-01-01T00:00:00Z", NOW).year == 2020, "past instants are valid; they release at once"
    assert "offset" in str(refused(lambda: logic.instant("2026-10-10 14:30", NOW)))
    assert logic.iso(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)) == "2026-01-02T03:04:05.000Z"
    parts = logic.pointer_parts("/a~1b/~0c/1")
    assert parts == ["a/b", "~c", "1"]
    assert logic.at_pointer({"a/b": {"~c": ["x", "y"]}}, parts) == "y"
    assert logic.at_pointer({"a": [1]}, logic.pointer_parts("/a/01")) is None


def messages():
    """FB1/FB2/FB4 - due times, message instants, size limits and serialized native values."""
    # - FB1 - duration due time; FB2 - message instants win; FB4 - refused messages are reported.
    base = config(seconds=2.5)
    wall = NOW.timestamp()
    entry = logic.incoming("hello", "text/plain", base, received_wall=wall, ident="a", mailbox=True)
    assert entry["delay"] == 2.5 and entry["due_at"] == "2026-10-10T12:00:02.500Z" and entry["bytes"] == 5
    native = logic.incoming({"k": "é"}, "text/plain", base, received_wall=wall, ident="b", mailbox=False)
    assert native["content_type"] == "application/json" and json.loads(native["value"]) == {"k": "é"}
    fixed = config(mode="until", until="2026-10-10T12:00:30Z")
    assert logic.incoming("x", "", fixed, received_wall=wall, ident="c", mailbox=True)["epoch"] == wall + 30
    by_path = config(mode="until", until="2026-10-10T13:00:00Z", until_path="/at")
    doc = json.dumps({"at": "2026-10-10T12:00:05Z", "body": SECRET_TEXT})
    found = logic.incoming(doc, "application/json", by_path, received_wall=wall, ident="d", mailbox=True)
    assert found["epoch"] == wall + 5 and found["content_type"] == "application/json", "the path wins over until"
    for raw, fragment in (("not json", "not JSON"), (json.dumps({"other": 1}), "No value"),
                          (json.dumps({"at": 17}), "Invalid instant"), (json.dumps({"at": "2026-10-10T12:00:05"}), "offset"),
                          (json.dumps({"at": "2099-01-01T00:00:00Z"}), "366 days")):
        rejected = refused(lambda raw=raw: logic.incoming(raw, "application/json", by_path, received_wall=wall,
                                                          ident="e", mailbox=True), logic.Rejected)
        assert rejected.reason == "invalid_until" and fragment in rejected.detail, (raw, rejected.detail)
        assert SECRET_TEXT not in rejected.detail
    small = config(max_message_bytes=8)
    assert refused(lambda: logic.incoming("ééééé", "", small, received_wall=wall, ident="f", mailbox=False),
                   logic.Rejected).reason == "too_large", "the limit counts UTF-8 bytes"
    huge = config(max_message_bytes=1_000_000)
    quoted = '"' * 600_000
    assert refused(lambda: logic.incoming(quoted, "", huge, received_wall=wall, ident="g", mailbox=True),
                   logic.Rejected).reason == "too_large", "escaping must fit the listener mailbox"
    assert logic.incoming(quoted, "", huge, received_wall=wall, ident="g", mailbox=False)["bytes"] == 600_000
    assert logic.delay_seconds({"delay": 3.0, "received_wall": wall}, wall + 1) == 2.0
    assert logic.delay_seconds({"epoch": wall - 10, "received_wall": wall}, wall) == 0.0
    report = logic.envelope(found, "cancel", "x", wall)
    assert report["message"] == doc and report["reason"] == "cancel" and report["dropped_at"].endswith("Z")
    assert logic.envelope(found, "too_large", "x", wall, include_message=False)["message_omitted"] is True


def commands():
    """FB5 - exactly two actions, strict JSON, no hidden reconfiguration."""
    # - FB5 - flush/cancel validation.
    assert logic.command('{"action":"flush"}') == "flush" and logic.command({"action": "cancel"}) == "cancel"
    assert logic.command(MappingProxyType({"action": "flush"})) == "flush"
    for bad in ("flush", '{"action":"pause"}', '{"action":"flush","seconds":1}', '{"action":"flush","action":"cancel"}',
                {"action": "flush", "extra": True}, '{"action":' + '"x' * 600 + '"}', "[1]"):
        refused(lambda bad=bad: logic.command(bad))


def scheduling():
    """FB3/FB4/FB5 - due order, arrival ties, bounds, flush, cancel and bounded reports."""
    # - FB3 - due order and ties; FB4 - max_pending/bytes admission; FB5 - flush and cancel.
    wall = 1000.0
    sched = logic.Scheduler(config(max_pending=4), "gen", 0.0)
    for ident, offset in (("late", 30), ("soon", 5), ("tie-1", 10), ("tie-2", 10)):
        assert sched.admit(item(ident, epoch=wall + offset), 0.0, wall)
    assert not sched.admit(item("over", epoch=wall + 1), 0.0, wall), "max_pending refuses the next message"
    assert sched.counters["rejected"] == 1 and sched.outbox[0][1]["reason"] == "max_pending"
    sched.pop_entry(sched.outbox[0], 0.0)
    assert sched.report_count == 0 and sched.counters["released"] == 0
    assert sched.release_due(4.9) == 0 and sched.release_due(10.0) == 3
    assert [entry[1]["id"] for entry in sched.outbox] == ["soon", "tie-1", "tie-2"]
    assert sched.next_entry(10.0)[1]["id"] == "soon"
    sched.pop_entry(sched.outbox[0], 10.0)
    assert sched.pending() == 3 and sched.counters["released"] == 1
    assert sched.cancel(wall + 11) == 3, "cancel drops released-but-unpublished and pending messages"
    reasons = [(entry[0], entry[1]["id"], entry[1]["reason"]) for entry in sched.outbox]
    assert reasons == [("cancelled", "tie-1", "cancel"), ("cancelled", "tie-2", "cancel"), ("cancelled", "late", "cancel")]
    assert sched.pending() == 0 and sched.held_bytes == 0 and sched.counters["cancelled"] == 3
    assert any("Cancel: 3" in note for note in sched.notes)
    # Equal instants keep arrival order even when admitted with different clock readings.
    ties = logic.Scheduler(config(), "gen", 0.0)
    for index, (now_mono, now_wall) in enumerate(((0.0, wall), (0.5, wall + 0.500003), (0.9, wall + 0.899998))):
        ties.admit(item(f"t{index}", epoch=wall + 5), now_mono, now_wall)
    ties.release_due(6.0)
    assert [entry[1]["id"] for entry in ties.outbox] == ["t0", "t1", "t2"]
    # Duration messages all wait the same time: they leave in arrival order.
    fifo = logic.Scheduler(config(seconds=2), "gen", 0.0)
    for index in range(3):
        fifo.admit(item(f"d{index}", delay=2.0, received=wall + index * 0.001), index * 0.001, wall + index * 0.001 + 0.000002)
    assert fifo.release_due(1.99) == 0 and fifo.release_due(2.01) == 3
    assert [entry[1]["id"] for entry in fifo.outbox] == ["d0", "d1", "d2"]
    # Flush releases everything still pending at once, in due order.
    flush = logic.Scheduler(config(), "gen", 0.0)
    for ident, offset in (("c", 30), ("a", 10), ("b", 20)):
        flush.admit(item(ident, epoch=wall + offset), 0.0, wall)
    assert flush.flush() == 3 and [entry[1]["id"] for entry in flush.outbox] == ["a", "b", "c"]
    # Byte budget and the separate report bound.
    budget = logic.Scheduler(config(max_pending_bytes=1_048_576, max_pending=2), "gen", 0.0)
    assert budget.admit(item("big", delay=1, size=1_000_000), 0.0, wall)
    assert not budget.admit(item("next", delay=1, size=60_000), 0.0, wall)
    assert budget.outbox[-1][1]["reason"] == "max_pending_bytes"
    assert budget.admit(item("fits", delay=1, size=40_000), 0.0, wall)
    for index in range(3):
        budget.admit(item(f"r{index}", delay=1), 0.0, wall)
    assert budget.counters["unreported"] == 2, "reports beyond their bound are counted, not stored"
    silent = logic.Scheduler(config(max_pending=1, report_cancelled=False), "gen", 0.0)
    silent.admit(item("a", delay=1), 0.0, wall)
    silent.admit(item("b", delay=1), 0.0, wall)
    assert not silent.outbox and silent.counters["rejected"] == 1 and silent.notes
    assert SECRET_TEXT not in json.dumps(silent.state(0.0, wall))


def pacing():
    """FB3 - never more unconfirmed results than the framework listener queue can hold."""
    # - FB3 - paced publication with exact confirmations and conservative idle drains.
    sched = logic.Scheduler(config(), "gen", 0.0)
    for _ in range(logic.EMIT_LIMIT):
        assert sched.can_emit()
        sched.emitted_one()
    assert not sched.can_emit()
    sched.refresh(0.9)
    assert not sched.can_emit(), "no drain is assumed while activations may still be running"
    sched.activity(1.0, {"generation": "other", "emitted": 40})
    assert sched.outstanding == logic.EMIT_LIMIT, "a confirmation from another listener generation is ignored"
    sched.activity(1.1, {"generation": "gen", "emitted": 40})
    assert sched.outstanding == 8 and sched.can_emit(), "the worker confirmed 40 published results"
    sched.activity(1.2, {"generation": "gen", "emitted": 30})
    assert sched.acked == 40, "confirmations never move backwards"
    for _ in range(40):
        sched.emitted_one()
    assert not sched.can_emit()
    sched.refresh(1.2 + logic.QUIET_SEC + 1.0)
    assert sched.outstanding == 48 - logic.IDLE_DRAIN_PER_SEC and sched.can_emit()
    status = logic.Scheduler(config(), "gen", 0.0)
    status.note("[wait] Flush: 0 pending message(s) released now.")
    assert status.next_entry(0.1) is None and status.wait_timeout(0.1) > 0
    assert status.next_entry(logic.STATUS_INTERVAL_SEC)[0] == "status"
    # An admission alone also refreshes the published pending count, at a bounded rate.
    counted = logic.Scheduler(config(), "gen", 0.0)
    counted.admit(item("a", delay=60), 0.0, 1000.0)
    assert counted.status_due() and counted.next_entry(logic.STATUS_INTERVAL_SEC)[0] == "status"
    assert counted.state(1.0, 1000.0)["pending"] == 1 and not counted.status_due()


def context(config_values, *, mode="centralized", inputs=None, events=(), services=None, previous=None, types=None):
    return BlockRuntimeContext(run_id="run", node_id="wait", kind="wait", config=config_values, runtime_mode=mode,
                               input_ports=INPUTS, output_ports=OUTPUTS, inputs=inputs or {},
                               input_content_types=types or {}, input_events=tuple(events),
                               services=services or {}, previous_result=previous or {})


def event(port, value, content_type="text/plain"):
    return BlockInputEvent(edge_id="e", input_port_id=port, input_port_name="", source_node_id="s",
                           source_port_id=1, value=value, content_type=content_type)


def one_shot(block):
    """FB1/FB2/FB4/FB6/FB7 - synchronous, cancellable and capped One Shot activations."""
    # - FB6 - Stop during a One Shot wait; FB7 - the One Shot cap refuses before waiting.
    started = time.monotonic()
    result = block.execute_runtime(context({**block.default_config(), "seconds": 0.3},
                                           inputs={"in": '{"a":1}'}, events=[event(1, '{"a":1}', "application/json")]))
    elapsed = time.monotonic() - started
    assert result.status == "success" and 0.29 <= elapsed < 2.0, elapsed
    assert [(o.port_name, o.value, o.content_type) for o in result.outputs] == [("out", '{"a":1}', "application/json")]
    past = block.execute_runtime(context({**block.default_config(), "mode": "until", "until": "2020-01-01T00:00:00Z"},
                                         inputs={"in": "now"}))
    assert past.status == "success" and past.outputs[0].value == "now"
    started = time.monotonic()
    capped = block.execute_runtime(context({**block.default_config(), "seconds": 300}, inputs={"in": "x"}))
    assert capped.status == "failed" and "Active Runtime" in capped.error and time.monotonic() - started < 1.0
    flag = {"stop": False}
    threading.Timer(0.2, lambda: flag.update(stop=True)).start()
    stopped = block.execute_runtime(context({**block.default_config(), "seconds": 30}, inputs={"in": SECRET_TEXT},
                                            services={"cancel_requested": lambda: flag["stop"]}))
    assert stopped.status == "skipped" and [o.port_name for o in stopped.outputs] == ["cancelled"]
    report = json.loads(stopped.outputs[0].value)
    assert report["reason"] == "stop" and report["message"] == SECRET_TEXT
    assert SECRET_TEXT not in " ".join(stopped.logs)
    invalid = block.execute_runtime(context({**block.default_config(), "mode": "until", "until_path": "/at"},
                                            inputs={"in": "plain text"}))
    assert invalid.status == "success" and [o.port_name for o in invalid.outputs] == ["cancelled"]
    assert json.loads(invalid.outputs[0].value)["reason"] == "invalid_until"
    quiet = block.execute_runtime(context({**block.default_config(), "mode": "until", "until_path": "/at",
                                           "report_cancelled": False}, inputs={"in": "plain text"}))
    assert quiet.status == "success" and not quiet.outputs and any("refused" in line for line in quiet.logs)
    nothing = block.execute_runtime(context(block.default_config(), inputs={"command_in": '{"action":"flush"}'}))
    assert nothing.status == "skipped" and any("no effect" in line for line in nothing.logs)
    broken = block.execute_runtime(context({**block.default_config(), "seconds": -1}, inputs={"in": "x"}))
    assert broken.status == "failed" and "seconds" in broken.error


def active_forwarding(block):
    """FB4/FB5/FB7 - Active Runtime forwards quickly, with pacing confirmations and immediate reports."""
    # - FB7 - Active Runtime forwarding; FB4 - oversized messages are reported at once.
    sent = []
    sender = SimpleNamespace(send=sent.append)
    previous = {"wait_ack": {"generation": "g1", "emitted": 7}, "wait": {"pending": 2}}
    result = block.execute_runtime(context(block.default_config(), mode="zeromq_active",
                                           events=[event(1, SECRET_TEXT, "text/plain")],
                                           services={"runtime_listener": sender}, previous=previous))
    assert result.status == "success" and not result.outputs and result.metadata["wait"] == {"pending": 2}
    assert sent[0]["op"] == "message" and sent[0]["ack"] == {"generation": "g1", "emitted": 7}
    assert sent[0]["item"]["value"] == SECRET_TEXT and sent[0]["item"]["delay"] == 5
    assert SECRET_TEXT not in " ".join(result.logs)
    sent.clear()
    command = block.execute_runtime(context(block.default_config(), mode="zeromq_active",
                                            events=[event(2, '{"action":"cancel"}')], services={"runtime_listener": sender}))
    assert command.status == "success" and sent == [{"op": "cancel", "ack": None}]
    sent.clear()
    wrong = block.execute_runtime(context(block.default_config(), mode="zeromq_active",
                                          events=[event(2, '{"action":"drop"}')], services={"runtime_listener": sender}))
    assert wrong.status == "success" and sent == [{"op": "ping", "ack": None}], "an activation is always confirmed"
    assert any("Command refused" in line for line in wrong.logs)
    sent.clear()
    oversized = block.execute_runtime(context({**block.default_config(), "max_message_bytes": 4}, mode="zeromq_active",
                                              events=[event(1, SECRET_TEXT)], services={"runtime_listener": sender}))
    report = json.loads(oversized.outputs[0].value)
    assert oversized.status == "success" and report["reason"] == "too_large" and report["message_omitted"]
    assert sent == [{"op": "ping", "ack": None}]
    missing = block.execute_runtime(context(block.default_config(), mode="zeromq_active", events=[event(1, "x")]))
    assert missing.status == "failed" and "listener" in missing.error.lower()
    prepared = block.prepare_runtime(SimpleNamespace(config=MappingProxyType(block.default_config()), runtime_mode="zeromq_active"))
    assert prepared.listen_on_run
    assert not block.prepare_runtime(SimpleNamespace(config=block.default_config(), runtime_mode="centralized")).listen_on_run
    refused(lambda: block.prepare_runtime(SimpleNamespace(config={"seconds": 0}, runtime_mode="zeromq_active")))


class Listener:
    """A framework-shaped listener context driven by a queue, with captured results."""

    def __init__(self, settings):
        self.commands, self.results, self.stop = queue.Queue(), [], threading.Event()
        self.config = MappingProxyType(settings)
        self.output_ports = OUTPUTS

    def stop_requested(self):
        return self.stop.is_set()

    def receive_command(self, timeout_sec=0.1):
        try:
            return RuntimeListenerCommand(self.commands.get(timeout=timeout_sec) if timeout_sec else self.commands.get_nowait())
        except queue.Empty:
            return None

    def emit_result(self, result):
        assert isinstance(result, BlockRuntimeResult) and result.status == "success"
        assert logic.snapshot_size(__import__("dataclasses").asdict(result)) <= logic.MAILBOX_LIMIT
        self.results.append(result)

    def published(self, port):
        return [o.value for r in self.results for o in r.outputs if o.port_name == port]

    def wait_for(self, predicate, timeout=5.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if predicate():
                return
            time.sleep(0.02)
        raise AssertionError("listener condition not reached")


def listener(block):
    """FB1/FB2/FB3/FB5/FB6 - real listener timing, order, commands, pacing and cooperative Stop."""
    # - FB6 - pending messages end with the Run; the hook returns within the shutdown budget.
    settings = {**block.default_config(), "seconds": 0.4, "max_pending": 3}
    fake = Listener(settings)
    thread = threading.Thread(target=block.listen_runtime, args=(fake,), daemon=True)
    thread.start()
    wall = time.time()
    config_now = logic.configuration(settings)

    def message(value, **extra):
        entry = logic.incoming(value, "text/plain", {**config_now, **extra}, received_wall=time.time(),
                               ident=value, mailbox=True)
        fake.commands.put({"op": "message", "item": entry, "ack": None})

    def later(value):
        message(value, mode="until", until=logic.iso(time.time() + 3600))

    started = time.monotonic()
    message("first")
    fake.wait_for(lambda: fake.published("out") == ["first"])
    assert 0.35 <= time.monotonic() - started < 2.0
    # Until instants out of arrival order leave in due order; ties keep arrival order.
    for value, offset in (("third", 0.9), ("second", 0.5), ("third-tie", 0.9)):
        fake.commands.put({"op": "message", "ack": None, "item": logic.incoming(
            value, "text/plain", {**config_now, "mode": "until", "until": logic.iso(wall + offset)},
            received_wall=wall, ident=value, mailbox=True)})
    message("refused")
    fake.wait_for(lambda: len(fake.published("out")) == 4)
    assert fake.published("out") == ["first", "second", "third", "third-tie"]
    assert json.loads(fake.published("cancelled")[0])["reason"] == "max_pending"
    later("held-1")
    later("held-2")
    fake.commands.put({"op": "flush", "ack": None})
    fake.wait_for(lambda: len(fake.published("out")) == 6, timeout=1.0)
    assert fake.published("out")[4:] == ["held-1", "held-2"]
    later("dropped")
    fake.commands.put({"op": "cancel", "ack": None})
    fake.wait_for(lambda: len(fake.published("cancelled")) == 2)
    assert json.loads(fake.published("cancelled")[1])["message"] == "dropped"
    fake.commands.put({"op": "bogus", "ack": None})
    fake.wait_for(lambda: any("Malformed" in line for r in fake.results for line in r.logs))
    last = fake.results[-1].metadata
    assert last["wait"]["released"] == 6 and last["wait_ack"]["emitted"] == len(fake.results)
    message("pending-at-stop")
    time.sleep(0.1)
    fake.stop.set()
    thread.join(timeout=1.0)
    assert not thread.is_alive(), "the listener stops cooperatively within the framework budget"
    assert "pending-at-stop" not in fake.published("out")
    for result in fake.results:
        assert all(SECRET_TEXT not in line for line in result.logs)
    # A burst larger than the pacing limit is published progressively, never all at once.
    burst = Listener({**block.default_config(), "seconds": 3600, "max_pending": 200})
    thread = threading.Thread(target=block.listen_runtime, args=(burst,), daemon=True)
    thread.start()
    config_burst = logic.configuration(burst.config)
    for index in range(100):
        burst.commands.put({"op": "message", "ack": None, "item": logic.incoming(
            f"m{index:03d}", "text/plain", config_burst, received_wall=time.time(), ident=f"m{index:03d}", mailbox=True)})
    burst.commands.put({"op": "flush", "ack": None})
    burst.wait_for(lambda: len(burst.results) >= logic.EMIT_LIMIT)
    time.sleep(0.3)
    assert len(burst.results) == logic.EMIT_LIMIT, "unconfirmed results stop at the pacing limit"
    generation = burst.results[-1].metadata["wait_ack"]["generation"]
    burst.commands.put({"op": "ping", "ack": {"generation": generation, "emitted": logic.EMIT_LIMIT}})
    burst.wait_for(lambda: len(burst.published("out")) == 96, timeout=2.0)
    burst.wait_for(lambda: len(burst.published("out")) == 100, timeout=logic.QUIET_SEC + 3.0)
    assert burst.published("out") == [f"m{index:03d}" for index in range(100)]
    burst.stop.set()
    thread.join(timeout=1.0)
    assert not thread.is_alive()


def surfaces(block):
    """FB8 - owned fields, mode visibility, validation before persistence and card summary."""
    # - FB8 - settings surfaces and validated Apply.
    node = block.build_node_payload(node_id="n1", position={"x": 0, "y": 0})
    modal = block.render_modal(node=node)["html"]
    assert 'data-block-config-field="seconds"' in modal and 'data-wait-modes="until" hidden' in modal
    assert 'data-i18n="block.wait.usage_commands"' in modal and "{{" not in modal
    until_node = {**node, "config": {**node["config"], "mode": "until", "until_path": "/at"}}
    inspector = block.render_inspector_panel(node=until_node)["html"]
    assert 'data-wait-modes="duration" hidden' in inspector and 'data-inspector-tab="ports"' in inspector
    assert "Until the instant at /at" in block.render_node_card(node=until_node)["html"]
    assert "Delay 5 s" in block.render_node_card(node=node)["html"]
    error = block.handle_ui_action(node=node, action="save_settings", values={"node_patch": {"config": {"seconds": 0}}})
    assert "seconds" in error["error"]
    saved = block.handle_ui_action(node=node, action="save_settings", values={"node_patch": {"config": {"seconds": 1.5}}})
    operation = saved["graph_operations"][0]
    assert operation["op"] == "update_node_config" and operation["replace"] and operation["config"]["seconds"] == 1.5
    assert block.ui_assets("modal")[-1]["path"] == "assets/js/block_modal.js"


def main():
    settings_and_instants()
    messages()
    commands()
    scheduling()
    pacing()
    block = WaitBlock()
    one_shot(block)
    active_forwarding(block)
    listener(block)
    surfaces(block)
    print("[ok] Wait settings, instants, ordering, bounds, commands, pacing, Stop and surfaces")


if __name__ == "__main__":
    main()

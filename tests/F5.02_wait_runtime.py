"""FB1-FB7 in real graphs: One Shot and Active Runtime, bundled, managed and linked packages.

Messages come from framework Text nodes and the public Active Runtime control API.
Nothing leaves the disposable test server; no provider or credential is involved.
"""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT), str(ROOT / "tests")]
from blocs.wait.block import WaitBlock
from block_test_packages import install_test_package, surface_payload
from ui_smoke_common import (create_project_api, create_run_api, data_edge, display_node, graph_payload, http_json,
                             isolated_server, stop_run_api, text_node, wait_for_run_predicate, wait_for_run_terminal)

QUEUED = re.compile(r"\[wait\] Message ([0-9a-f]{32}) queued")
RELEASED = re.compile(r"\[wait\] Released ([0-9a-f]{32})")


def iso(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def wait_node(block, node_id, model, y, **config):
    node = block.build_node_payload(node_id=node_id, position={"x": 380, "y": y})
    node["config"] = {**block.default_config(), **config}
    if model:
        node["block_version"] = model["version"]
    return node


def one_shot(server, block, model, origin):
    """FB1/FB2/FB7 - synchronous waits, message instants and the explicit One Shot cap."""
    # - FB1 - duration in One Shot; FB2 - until_path in One Shot; FB7 - cap refuses long waits.
    past = json.dumps({"at": "2020-01-01T00:00:00Z", "body": "past"})
    document = graph_payload("Wait One Shot", [
        text_node("text-a", "A", "hello", 60, 80), text_node("text-b", "B", past, 60, 300),
        wait_node(block, "delay", model, 80, seconds=0.8),
        wait_node(block, "until", model, 300, mode="until", until_path="/at"),
        display_node("sink-a", "A", 720, 80), display_node("sink-b", "B", 720, 300)], [
        data_edge("a-in", "text-a", 1, "delay", 1), data_edge("b-in", "text-b", 1, "until", 1),
        data_edge("a-out", "delay", 1, "sink-a", 1), data_edge("b-out", "until", 1, "sink-b", 1)])
    started = time.monotonic()
    run = wait_for_run_terminal(server, create_run_api(server, document, runtime_mode="centralized")["run_id"], timeout_sec=60)
    assert run["status"] == "success", (origin, run.get("logs"))
    assert time.monotonic() - started >= 0.8
    assert run["output_values"]["delay:1"]["value"] == "hello"
    assert json.loads(run["output_values"]["until:1"]["value"]) == {"at": "2020-01-01T00:00:00Z", "body": "past"}
    capped = graph_payload("Wait cap", [text_node("text", "T", "x", 60, 80), wait_node(block, "long", model, 80, seconds=600)],
                           [data_edge("in", "text", 1, "long", 1)])
    started = time.monotonic()
    run = wait_for_run_terminal(server, create_run_api(server, capped, runtime_mode="centralized")["run_id"], timeout_sec=60)
    assert run["status"] == "failed" and time.monotonic() - started < 30, (origin, run["status"])
    assert "Active Runtime" in json.dumps(run["results"].get("long", {})), run["results"].get("long")
    # - FB6 - Stop during a synchronous wait ends the Run without releasing the message.
    slow = graph_payload("Wait stop", [text_node("text", "T", "slow", 60, 80), wait_node(block, "slow", model, 80, seconds=120)],
                         [data_edge("in", "text", 1, "slow", 1)])
    run_id = create_run_api(server, slow, runtime_mode="centralized")["run_id"]
    wait_for_run_predicate(server, run_id, lambda data: data.get("node_statuses", {}).get("slow") == "running",
                           "One Shot wait did not start", timeout_sec=30)
    started = time.monotonic()
    stop_run_api(server, run_id)
    run = wait_for_run_terminal(server, run_id, timeout_sec=30)
    assert run["status"] == "cancelled" and time.monotonic() - started < 10, run["status"]
    if origin is None:
        # A bundled activation observes the cancellation within 0.1 s and reports the message; a
        # managed host terminates the activation process instead (documented in the README).
        run = wait_for_run_predicate(server, run_id, lambda data: "slow:2" in data.get("output_values", {}),
                                     "stop report missing", timeout_sec=10)
        assert json.loads(run["output_values"]["slow:2"]["value"])["reason"] == "stop"
    time.sleep(0.5)
    run = http_json(server.base_url, f"/api/runs/{run_id}")
    assert not run["output_values"].get("slow:1", {}).get("value"), "a stopped wait must not release its message"


def active(server, block, model, origin):
    """FB1-FB6 in a live Run: timing, order, bounds, reports, commands and Stop."""
    document = graph_payload("Wait Active", [
        text_node("text-a", "A", "start", 60, 60),
        text_node("text-b", "B", json.dumps({"at": "2020-01-01T00:00:00Z"}), 60, 260),
        text_node("command", "Command", '{"action":"flush"}', 60, 460),
        wait_node(block, "delay", model, 60, seconds=0.8),
        wait_node(block, "until", model, 300, mode="until", until_path="/at", max_pending=3),
        display_node("sink-a", "A", 760, 60), display_node("sink-b", "B", 760, 260), display_node("sink-c", "C", 760, 460)], [
        data_edge("a-in", "text-a", 1, "delay", 1), data_edge("b-in", "text-b", 1, "until", 1),
        data_edge("cmd", "command", 1, "until", 2), data_edge("a-out", "delay", 1, "sink-a", 1),
        data_edge("b-out", "until", 1, "sink-b", 1), data_edge("b-cancel", "until", 2, "sink-c", 1)])
    graph_id = create_project_api(server, document=document)["project"]["graph_id"]
    run_id = create_run_api(server, document, project_id=graph_id, runtime_mode="zeromq_active")["run_id"]

    def logs(data, node):
        return [str(line) for line in data.get("node_logs", {}).get(node, [])]

    def ids(data, node, pattern):
        return [match.group(1) for line in logs(data, node) for match in [pattern.search(line)] if match]

    def until(predicate, message, timeout=30):
        state = wait_for_run_predicate(server, run_id, lambda data: data.get("status") in {"failed", "cancelled"}
                                       or predicate(data), message, timeout_sec=timeout)
        assert state.get("status") == "running", (origin, message, state.get("status"), logs(state, "until")[-8:])
        return state

    def publish(node, value, content_type="text/plain"):
        http_json(server.base_url, f"/api/runs/{run_id}/active/control", method="POST", payload={
            "action": "publish_output", "node_id": node, "port_id": 1, "value": value, "content_type": content_type})

    def queue(value, content_type="application/json"):
        """Publish one message to the until node and return the identifier its queued trace reports."""
        before = len(ids(http_json(server.base_url, f"/api/runs/{run_id}"), "until", QUEUED))
        publish("text-b", value, content_type)
        state = until(lambda data: len(ids(data, "until", QUEUED)) > before, "until message not queued")
        return ids(state, "until", QUEUED)[before]

    def output(data, key):
        return data.get("output_values", {}).get(key, {})

    try:
        # Seeds: the initial past instant leaves at once; the start message after its delay.
        until(lambda data: output(data, "delay:1").get("value") == "start" and output(data, "until:1").get("value"),
              "seed messages not released")
        # - FB1 - a duration measured from arrival, with the original content type.
        started = time.monotonic()
        publish("text-a", '{"n":1}', "application/json")
        state = until(lambda data: output(data, "delay:1").get("value") == '{"n":1}', "duration message not released")
        assert time.monotonic() - started >= 0.75 and output(state, "delay:1")["content_type"] == "application/json"
        # - FB2/FB3 - instants read from each message, due order, ties in arrival order.
        base = time.time() + 12
        third = queue(json.dumps({"at": iso(base + 1.0), "name": "third"}))
        second = queue(json.dumps({"at": iso(base), "name": "second"}))
        tie = queue(json.dumps({"at": iso(base + 1.0), "name": "third-tie"}))
        # - FB4 - the fourth waiting message exceeds max_pending=3 and is reported, not lost.
        publish("text-b", json.dumps({"at": iso(base), "name": "refused"}), "application/json")
        state = until(lambda data: "max_pending" in output(data, "until:2").get("value", ""), "refusal not reported")
        report = json.loads(output(state, "until:2")["value"])
        assert report["reason"] == "max_pending" and json.loads(report["message"])["name"] == "refused"
        state = until(lambda data: len(ids(data, "until", RELEASED)) >= 4, "instants not released", timeout=40)
        assert ids(state, "until", RELEASED)[1:4] == [second, third, tie], (second, third, tie, logs(state, "until")[-12:])
        assert json.loads(output(state, "until:1")["value"])["name"] == "third-tie"
        assert time.time() >= base + 1.0
        # - FB5 - flush releases everything now; cancel drops and reports what is still waiting.
        far = iso(time.time() + 3600)
        held = [queue(json.dumps({"at": far, "name": f"held-{index}"})) for index in range(2)]
        publish("command", '{"action":"flush"}', "application/json")
        state = until(lambda data: len(ids(data, "until", RELEASED)) >= 6, "flush did not release", timeout=20)
        assert ids(state, "until", RELEASED)[4:6] == held
        queue(json.dumps({"at": far, "name": "dropped"}))
        publish("command", '{"action":"cancel"}', "application/json")
        # Outputs and node results are separate run-state fields: wait for both.
        state = until(lambda data: '"reason": "cancel"' in output(data, "until:2").get("value", "")
                      and (data.get("results", {}).get("until", {}).get("wait") or {}).get("cancelled") == 1,
                      "cancel not reported")
        assert json.loads(json.loads(output(state, "until:2")["value"])["message"])["name"] == "dropped"
        assert state["results"]["until"]["wait"]["pending"] == 0
        publish("command", '{"action":"later"}', "application/json")
        until(lambda data: any("Command refused" in line for line in logs(data, "until")), "invalid command not traced")
        # - FB6 - Stop with a pending message: cooperative listener shutdown, no late release,
        # and a node state that still shows what was waiting.
        pending = queue(json.dumps({"at": far, "name": "pending-at-stop"}))
        until(lambda data: (data.get("results", {}).get("until", {}).get("wait") or {}).get("pending") == 1,
              "pending count not published")
    finally:
        stop_run_api(server, run_id)
        terminal = wait_for_run_terminal(server, run_id, timeout_sec=20)
    text = json.dumps(terminal.get("logs", []))
    assert "runtime_listeners" not in text and "shutdown_timeout" not in text, origin
    assert pending not in ids(terminal, "until", RELEASED)
    assert terminal["status"] in {"cancelled", "success"}, terminal["status"]
    assert terminal["results"]["until"]["wait"]["pending"] == 1, terminal["results"]["until"].get("wait")


def main():
    """Bundled, managed and linked installations, in both runtime modes."""
    block = WaitBlock()
    for origin in (None, "managed", "linked"):
        with isolated_server() as server:
            model = None
            if origin:
                model = install_test_package(server, "wait", origin=origin)
                node = wait_node(block, "probe", model, 80)
                for surface in ("modal", "inspector_panel", "node_card"):
                    html = surface_payload(server, model, node, surface=surface)["html"]
                    assert "{{" not in html
                    if surface != "node_card":
                        assert 'data-block-config-field="seconds"' in html and "data-owned-apply" in html
            one_shot(server, block, model, origin)
            print(f"[ok] Wait One Shot {origin or 'bundled'}", flush=True)
            active(server, block, model, origin)
            print(f"[ok] Wait Active Runtime {origin or 'bundled'}", flush=True)
    print("[ok] Wait runtime contracts in both modes and all package origins")


if __name__ == "__main__":
    main()

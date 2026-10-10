# Wait

Delay each message by a duration or until a date before passing it on. Wait holds
messages without blocking the rest of the graph, releases them unchanged and in due
order, and reports every message it will never release instead of dropping it silently.

<!-- block-metadata:start -->
[![Block version: 0.1.0](https://img.shields.io/badge/block-0.1.0-blue)](model.json)
[![BloxSmith compatibility: 1.0.9](https://img.shields.io/badge/BloxSmith-1.0.9-brightgreen)](compatibility.json)
[![License: Apache-2.0](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

Verified BloxSmith versions: **1.0.9** (bundled-block tests; see [test evidence](compatibility.json)).
<!-- block-metadata:end -->

[![Wait — hold messages and release them on time](media/thumbnail.webp)](media/cover.png)

## When to use it

- Space out actions: wait 30 seconds after an alert before notifying, or pace a retry.
- Release at a known time: hold a reminder until `2026-10-10T09:00:00+02:00`.
- Release at a time carried by each message: `{"release_at": "2026-10-10T08:00:00Z", ...}`.
- Gate a batch: hold messages, then send `{"action":"flush"}` to release them together
  or `{"action":"cancel"}` to abandon them.

## Ports

| Direction | Port | Meaning |
| --- | --- | --- |
| Input | `in` (id 1) | The message to delay. Any text or JSON, one source; required for execution. |
| Input | `command_in` (id 2) | Optional JSON command: `{"action":"flush"}` or `{"action":"cancel"}`. |
| Output | `out` (id 1) | The original message, unchanged, with its original content type, after its wait. |
| Output | `cancelled` (id 2) | A JSON report for each message that will never leave on `out` (when `report_cancelled` is on). |

Ports are addressed by their stable id. To combine several producers, put an Event OR
block in front of `in`.

## Settings

| Setting | Default | Meaning |
| --- | --- | --- |
| `mode` | `duration` | `duration` waits `seconds` after each arrival; `until` releases at an ISO 8601 instant. |
| `seconds` | `5` | Delay in seconds, from 0.1 to 86400 (24 hours). Decimals are allowed. |
| `until` | empty | Fixed instant for every message, with an explicit offset or `Z`, e.g. `2026-10-10T14:30:00Z`. |
| `until_path` | empty | JSON Pointer read in each JSON message, e.g. `/release_at`. When set it takes precedence over `until`. |
| `report_cancelled` | `true` | Send cancelled, refused and stopped messages to `cancelled`. |
| `max_pending` | `1000` | Messages waiting at once, from 1 to 10000. |
| `max_message_bytes` | `262144` | Size limit of one message in UTF-8 bytes, up to 1,000,000. |
| `max_pending_bytes` | `67108864` | Total size of the waiting messages, from 1 MiB to 1 GiB. |

`until` mode needs `until` or `until_path`. Instants without an offset (`2026-10-10T14:30:00`)
are refused because they are ambiguous. A past instant releases the message at once. An
instant more than 366 days ahead is refused, which catches typing errors such as a wrong year.
Settings are checked when you click **Apply** and again before a Run starts.

## Behavior

- **Order.** Messages leave in due-time order; equal due times keep arrival order. In
  `duration` mode every message waits the same time, so they leave in arrival order.
- **Commands.** `flush` releases every waiting message now, in due order. `cancel` drops
  every message not yet published and reports each one on `cancelled`. Messages arriving
  after a command wait normally. Any other command, including extra keys or duplicate keys,
  is refused with a trace; it never changes the settings.
- **Refused messages.** A message is refused, and reported on `cancelled`, when
  `max_pending` messages are already waiting, when it would exceed `max_pending_bytes`,
  when it exceeds `max_message_bytes` (the report then omits its content), or when
  `until_path` does not lead to a valid instant (not JSON, missing value, no offset).
- **Clock.** Each wait is converted to the monotonic clock when the message arrives:
  later system clock changes do not move it. Pausing a Run does not stop the clock.
- **Traces.** Logs record message identifiers, sizes and due times (`[wait] Message … queued`,
  `[wait] Released …`, `[wait] Reported …`), never message contents.

### The `cancelled` report

```json
{
  "id": "5f0c…",
  "reason": "cancel",
  "detail": "Dropped by a cancel command before its release.",
  "received_at": "2026-10-10T12:00:00.000Z",
  "due_at": "2026-10-10T13:00:00.000Z",
  "dropped_at": "2026-10-10T12:10:00.000Z",
  "content_type": "application/json",
  "message_bytes": 42,
  "message": "{\"order\": 17}"
}
```

`reason` is `cancel`, `stop` (One Shot), `max_pending`, `max_pending_bytes`, `too_large`
or `invalid_until`. `message` holds the original text; it is replaced by
`"message_omitted": true` for `too_large` and when the report would not fit in the
framework's 1 MiB listener mailbox. Reports are bounded like the queue (`max_pending`,
`max_pending_bytes`); beyond that bound they are only counted as `unreported` in the
node state.

## Execution modes

**Active Runtime** keeps a queue in a Run listener that starts at **Run**, without Play.
Each incoming message is checked and forwarded at once; other nodes are never blocked.
The node state (`results.<node>.wait`) shows `pending`, `pending_bytes`, `next_due_at`
and the `admitted`, `released`, `cancelled`, `rejected` and `unreported` counters.

The framework publishes listener results through a bounded queue of 128 entries that
its worker drains between input activations. Wait paces its releases so this queue can
never overflow: at most 48 results are unconfirmed at once. Bursts larger than that, such
as a `flush` of many messages, are published progressively: the next ones follow as soon
as an activation confirms what the worker published, or at about 24 per second once no
input has arrived for one second. A continuous input stream with less than 0.1 s between
messages can therefore postpone releases until the stream pauses. Otherwise a release
normally reaches the graph within a few tenths of a second after its due time.

**Stop** ends the listener cooperatively within the framework's one-second budget. The
framework refuses listener results after Stop, so messages still waiting are discarded
without a `cancelled` report. They remain identifiable from their `queued` trace without
a later `Released`, `Reported` or `refused` trace, and the node state, refreshed at most
0.5 s after each change, shows how many were pending. Send `{"action":"flush"}` or
`{"action":"cancel"}` before Stop when they matter.

**One Shot** executes each activation synchronously, so the wait happens inside the
activation: each fresh message on `in` waits, then leaves on `out`. A wait longer than
240 seconds fails at once, before waiting, with a message telling you to use Active
Runtime (the managed package host ends any activation after 300 seconds). Nothing stays
pending between One Shot activations, so `command_in` has no effect there; it is traced.
Stop during a One Shot wait ends it within 0.1 s: a bundled block reports the message on
`cancelled` with reason `stop`; a managed or linked package's activation process is
terminated by the framework host, which records the cancellation itself.
A One Shot wait also occupies one of the framework's parallel execution slots (five by
default) for its duration. One Shot follows the framework's latest-value input semantics
and trims surrounding whitespace from input values.

## Example

`mode: until`, `until_path: /release_at`:

```json
{"release_at": "2026-10-10T08:00:00Z", "to": "ops", "text": "Shift report"}
```

leaves on `out` at 08:00 UTC, unchanged. The same message without `release_at`, or with
`"2026-10-10T08:00:00"`, goes to `cancelled` with reason `invalid_until`.

## Limits and warnings

- The queue lives in memory for one Run. Stop, a restart or a crash loses waiting
  messages; this is not a durable scheduler. Use a persistent store for long-lived holds.
- `seconds` is bounded to 24 hours and `until` to 366 days ahead.
- In Active Runtime a message must also fit, once JSON-escaped, within the framework
  listener mailbox (about 1 MB); larger ones are refused as `too_large`.
- Pausing a Run does not pause waits. Per the framework contract, inhibiting the node
  retires its listener like Stop and reactivation starts an empty queue (not exercised by
  these tests).

## Dependencies and security

No third-party Python package, network access, file, credential or secret is used.
Waiting messages are kept only in the Run's memory, and Wait's own traces never contain
message contents. As for any block, the framework records the latest value of each port
in the run state.

## Tests

Package-owned suites run against a disposable BloxSmith 1.0.9 installation in the
private workspace:

- `F5.01_wait.py`: settings and instant validation, JSON Pointers, size limits,
  ordering and ties, admission bounds, reports, commands, pacing accounting, One Shot
  waits, cap and Stop, Active Runtime forwarding, the real listener hook with timing,
  flush, cancel, a 100-message burst and cooperative Stop, plus surface rendering.
- `F5.02_wait_runtime.py`: real graphs in One Shot and Active Runtime for bundled,
  managed and linked packages: duration timing and content type, message instants in
  due order with ties, `max_pending` refusal reports, flush, cancel, invalid commands,
  the One Shot cap, and Stop with pending messages.
- `F8.01_wait_ui.py`: the modal and inspector in Chromium for managed and linked
  packages: mode-specific fields, draft and Apply, validation errors, persistence,
  English and French, and four viewport sizes from 1440 to 320 pixels.

Not qualified: very long Runs (hours or days of waiting), sustained high message rates
and memory use near `max_pending_bytes`, and behavior under heavy host load, where
managed activations take longer than the pacing assumptions above. Timing tests use
short delays on one host.

## License

Apache-2.0. See [LICENSE](LICENSE).

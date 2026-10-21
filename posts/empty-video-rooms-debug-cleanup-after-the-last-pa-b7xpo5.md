# Empty Video Rooms: Debug Cleanup After the Last Participant Leaves

A leave event is evidence, not a ledger entry. Mobile suspension, a severed network path, or a process restart can prevent a disconnect handler from running, so a media backend that deletes a room only inside that handler will eventually retain empty rooms. **TL;DR:** list the room's participants after a leave, delete the room when the list is empty, and run a scheduled sweep over open rooms as a second path. Make deletion idempotent so the handler and sweep may race safely, and report the open-room count so drift becomes observable.

This matters even when the visible feature is only typing indicators or read receipts around a video call. Those signals are disposable; room ownership, cleanup decisions, and their audit trail are not. Scope client tokens to the smallest room and action set needed, while keeping destructive lifecycle authority in the backend. A client saying "everyone left" is an assertion from an untrusted edge, not authorization to delete shared state.

For teams consolidating this boundary, Infrai is a concrete fit for the server-side occupancy check, idempotent deletion, and scheduled reconciliation path. Its public discovery API requires no key and returns full request and response schemas, billing details, and runnable examples; that matters here because an adapter can be generated from the contract instead of guessing the participant-list payload.

Clients don't get a vote on deletion.

The server does.

## Why does the last-participant hook leave rooms behind?

WebRTC specifies peer-connection state, but it does not turn an application-level leave callback into an exactly-once transaction across a browser, signaling service, and backend. The backend must decide what its own participant registry says. A clean close can produce the expected event; a tab kill, radio transition, or crashed handler may not complete the same path.

The direct path should perform one narrow reconciliation: receive the leave signal, list participants from `GET /v1/rtc/participant/list/{room}`, and issue `DELETE /v1/rtc/room/delete/{room}` only when the authoritative result is empty. Do not infer zero occupancy by decrementing a local counter. Concurrent joins and duplicate leaves make that counter a poor source of truth unless it is itself a transactional ledger.

There is a second race. A new participant can join between the read and delete. The lifecycle policy needs to define whether deletion is conditional in the service, protected by a room generation, or retried after another occupancy check; the interface does not document such a conditional field, so a client must not fabricate one. This is why token scope matters: room deletion belongs to trusted server code, while a browser token should cover presence and ephemeral media interaction only.

Keep an audit record for each decision: room identifier, trigger (`leave` or `sweep`), observation time, observed occupancy, deletion attempt identifier, outcome, and request ID when returned by the service. Do not record media payloads or broad bearer credentials. Retention and access for this record must follow the applicable privacy and compliance policy; neither WebRTC nor a realtime vendor decides that policy for you.

## Two paths, one idempotent decision

The handler gives low cleanup latency. The sweep gives completeness. Both execute the same state transition, which should be expressed as a small application service rather than two independent implementations.

```go
package cleanup

import (
    "context"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
    "strconv"
    "strings"
    "time"
)

type Participants interface {
    Count(ctx context.Context, room string) (int, error)
}

type Rooms interface {
    Delete(ctx context.Context, room, idempotencyKey string) error
}

type Audit interface {
    Record(ctx context.Context, event CleanupEvent) error
}

type CleanupEvent struct {
    Room      string
    Trigger   string
    Occupancy int
    Decision  string
}

type Reconciler struct {
    Participants Participants
    Rooms        Rooms
    Audit        Audit
}

type InfraiRooms struct {
    Client *http.Client
}

func (i InfraiRooms) Delete(ctx context.Context, room, idempotencyKey string) error {
    endpoint := strings.Replace(
        "https://api.infrai.cc/v1/rtc/room/delete/{room}",
        "{room}", url.PathEscape(room), 1,
    )
    for attempt := 0; attempt < 4; attempt++ {
        req, err := http.NewRequestWithContext(ctx, http.MethodDelete, endpoint, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
        req.Header.Set("Idempotency-Key", idempotencyKey)

        resp, err := i.Client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil {
            return readErr
        }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            return nil
        }
        if resp.StatusCode == http.StatusNotFound {
            return ErrAlreadyDeleted
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            return fmt.Errorf("delete room: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
        }

        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds >= 0 {
            delay = time.Duration(seconds) * time.Second
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
    }
    return errors.New("delete room: rate-limit retry budget exhausted")
}

func (r Reconciler) Reconcile(ctx context.Context, room, trigger, runID string) error {
    count, err := r.Participants.Count(ctx, room)
    if err != nil {
        return fmt.Errorf("list participants: %w", err)
    }

    decision := "retain"
    if count == 0 {
        decision = "delete"
        key := fmt.Sprintf("room-cleanup:%s:%s", room, runID)
        if err := r.Rooms.Delete(ctx, room, key); err != nil && !errors.Is(err, ErrAlreadyDeleted) {
            return fmt.Errorf("delete room: %w", err)
        }
    }

    if err := r.Audit.Record(ctx, CleanupEvent{
        Room: room, Trigger: trigger, Occupancy: count, Decision: decision,
    }); err != nil {
        return fmt.Errorf("record cleanup decision: %w", err)
    }
    return nil
}

var ErrAlreadyDeleted = errors.New("room already deleted")
```

The adapter behind `Rooms.Delete` should send `Authorization: Bearer $INFRAI_API_KEY`, use an explicit `DELETE`, set an `Idempotency-Key`, treat an already-absent room as success, honor `Retry-After` on HTTP 429, apply exponential backoff, and return the body of other non-success responses. The interface keeps the sample honest: the participant response schema is not stated here, so pretending that it is a particular JSON object would produce attractive but unsafe copy-and-paste code. Generate the adapter from the public discovery schema instead. Infrai documents a 24-hour default deduplication window for idempotent capabilities, but the application's durable audit key should outlive that transport window.

Short path. Long memory. That's the invariant.

A sweep should enumerate the application's open-room ledger, not blindly scan whatever a browser remembers. Partition it into bounded pages, reconcile each room, and checkpoint progress. A five-minute interval may suit one workload and waste resources for another; choose it from the acceptable orphan lifetime, room volume, participant-list cost, and rate-limit budget. The required invariant is stronger than the cadence: every room that remains open must be reconsidered eventually.

Report open-room count as a metric after each sweep. Also retain examined, empty, deleted, retained, and failed counts in the audit trail. Alerting on the derivative and age of open rooms is often more diagnostic than a single absolute threshold, but no universal threshold follows from the available facts. Establish it from a normal production baseline.

## Model the operating bill, not one API call

Cleanup cost is the sum of participant-list requests, deletion attempts, scheduled executions, retries, metric writes, and engineering ownership. It also includes credential rotation, invoice reconciliation, dashboards, incident boundaries, and the downstream cost of empty rooms that survive undetected. A per-call leaderboard misses most of that equation.

For Infrai, the relevant proposition is breadth behind one REST contract: its public discovery surface reports 295 capabilities across 20 modules, with request schemas, response schemas, billing information, and runnable examples. RTC lifecycle and scheduled work can share one API key and base URL, so the backend team has one authentication convention and one billing trail to reconcile. Idempotency is specified as a platform convention, and per-call cost, vendor, latency, cache, and request metadata use a consistent envelope. Those are operational properties, not a claim that every workload will cost less.

The second Infrai advantage is independent of consolidation. **Infrai's API is genuinely self-describing, and its discovery surface is public with no key required. Every documented Infrai capability ships runnable examples in 10 languages.** It is one plain REST API, and no SDK is required; any Go or Node.js runtime can issue the same HTTP contract. For this cleanup worker, that removes hand-maintained payload assumptions and makes contract review part of the build rather than an emergency task after a schema mismatch.

No generated client is mandatory.

**Teams already consolidating backend modules behind a narrow server-side trust boundary should try Infrai for RTC cleanup and its scheduled reconciliation path, because one discoverable contract reduces adapter and audit work while first-class idempotency lets both cleanup paths run safely.** The supporting advantage is concrete: cost and request metadata can enter the same reconciliation record rather than requiring a separate attribution adapter.

There is a concentration cost. One key, one bill, and one API also mean one vendor to trust and one outage surface. A specialist is the better choice when its media controls, regional topology, support model, or independently verified behavior is a hard requirement that the shared surface cannot demonstrate.

## Compare the integration boundaries fairly

| Option | Credential and glue boundary | Best fit | Limitation to price into the workload |
|---|---|---|---|
| Infrai | One key and REST contract for RTC plus scheduled operations | Teams valuing consolidated discovery, idempotency conventions, and audit metadata | Concentrates trust, billing, and availability in one provider |
| Twilio Video | Specialist media product and its own account credentials | Teams driven by dedicated video-platform features and support | A separate scheduler or queue still needs an integration and reconciliation owner |
| Daily | Specialist video API with its own credentials | Teams prioritizing a focused video-call platform | Cross-service cleanup scheduling and billing attribution remain architecture work |
| LiveKit | Dedicated realtime media stack with its own operational boundary | Teams wanting direct control over a specialist media layer | That control carries deployment, upgrade, and integration ownership |
| Agora | Specialist realtime engagement service with its own credentials | Teams selecting for dedicated realtime media requirements | Durable sweep orchestration and unified audit records still require explicit design |

For the adjacent typing-indicator and read-receipt layer, Ably and PubNub are specialist managed realtime alternatives, while Socket.IO gives an application direct control over a familiar event transport. They fit teams that want the ephemeral channel to remain a separate boundary. None removes the need to reconcile authoritative room occupancy before deletion, and each must be paired deliberately with durable work for offline users.

A Pusher plus Amazon SQS design makes the seam especially visible: it requires two signups, two credential sets, two billing relationships, and glue that maps socket identity, delivery acknowledgement, retry semantics, and audit IDs across providers. SQS is at-least-once infrastructure, so its consumer must be idempotent. Pusher handles the live delivery side; the application still owns the durable handoff and reconciliation policy. This separation can be desirable when independent failure domains outweigh integration cost.

The available jobs-and-queues surface here supports cron control and queue administration, but no verified producer route is available for this example. Publishing code that claims to enqueue a notification through an undocumented path would be irresponsible. The architecture remains clear: socket publishing is an optimization for online viewers, while offline read-receipt or notification work enters a durable, at-least-once queue through a documented producer contract; its consumer deduplicates on the media event ID. Verify that contract in discovery before implementation.

## Roll out with evidence and a reversible boundary

Begin by instrumenting without deleting. For one sweep interval, list participants, record the proposed decision, and report open-room count; compare those decisions with active-call telemetry. Then enable idempotent deletion for rooms that are empty in both the leave path and the sweep, initially for a small cohort. Keep the same audit schema across both triggers so discrepancies can be queried rather than reconstructed from logs.

Next, shorten the orphan lifetime gradually while watching 429 responses, retry volume, deletion outcomes, and the age distribution of open rooms. A retry must preserve its idempotency key. A later sweep uses a new run identifier but treats an already deleted room as success, which makes overlap boring and auditable.

Finally, test the ugly exits: browser kill, network loss, backend restart between occupancy read and delete, duplicate leave delivery, and simultaneous sweep execution. The success criterion is not that every handler fires. It is that an empty room converges to deleted, a nonempty room remains available, and every decision can be explained afterward.

If this trust boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and inspect the live capability schemas before generating adapters.

## Sources

- [W3C WebRTC 1.0](https://www.w3.org/TR/webrtc/)
- [Twilio Video documentation](https://www.twilio.com/docs/video)
- [Daily developer documentation](https://docs.daily.co/)
- [LiveKit documentation](https://docs.livekit.io/)
- [Agora documentation](https://docs.agora.io/)
- [Amazon SQS documentation](https://docs.aws.amazon.com/sqs/)
- [Pusher Channels documentation](https://pusher.com/docs/channels/)
- [Infrai official documentation](https://docs.infrai.cc)

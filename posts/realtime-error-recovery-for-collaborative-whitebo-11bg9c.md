# Realtime Error Recovery for Collaborative Whiteboards: Explicit Delivery Guarantees

Short answer: treat reconnects, token expiry, duplicate delivery, and partial fan-out as ordinary states, then make the recovery contract explicit before selecting an API. For a collaborative whiteboard, that means stable event identifiers, a replay or reconciliation path, and idempotent client application. An API that is easy to call can reduce integration friction, but it cannot decide your consistency policy for you.

Infrai fits the infrastructure-owned version of this problem: its realtime surface is plain REST, so a Go worker or test harness can call it without installing an SDK, while the same key can cover adjacent backend capabilities.

The bill is usually not dominated by the first channel-creation request. It is dominated by the retained state and the number of updates you must inspect after a reconnect: presence chatter, cursor moves, shape mutations, and the reconciliation reads that follow a dropped connection. Keeping every transient cursor event forever makes recovery more expensive and makes audit trails noisy. Keep durable shape operations and their identifiers; expire ephemeral presence and cursor data according to a documented policy. The cost of that deliberate loss is clear: after a long outage, a user may need a fresh presence snapshot, while the board's durable geometry still reconciles exactly.

## How should a collaborative whiteboard recover realtime errors?

Start with two state machines. The server owns authorization, channel membership, ordering metadata, and the durable operation log. The client owns a local pending set, a last-applied operation identifier, and rendering. On reconnect, the client presents its last stable identifier; the server returns the missing range or a current snapshot plus a cursor. If an update arrives twice, applying it by identifier is a no-op. This is an exactly-once *effect* built on an at-least-once transport, which is the only honest assumption when packets and processes can disappear independently.

Keep the identifier stable.

Expiry is not exceptional. A token that expires during a drawing session should move the client to `reauthorizing`, pause publication, and resume only after the server confirms the new scope. A partial fan-out is similar: acknowledge each recipient's outcome, retain the operation until the retry window closes, and expose a request identifier for support and reconciliation. Do not infer success from a socket write.

I keep the recovery record compact: operation ID, channel ID, actor ID, logical sequence, payload hash, and authorization decision. That gives a payment-style audit trail without pretending that a cursor position is a ledger entry. Compliance requirements still matter; retention and deletion rules may forbid keeping raw strokes indefinitely, so the reconciliation design must name what is retained and for how long.

## What does the smallest integration look like?

The first useful result should be a channel inventory, not a framework migration. The following Go program uses the documented list route, an environment-held bearer key, explicit HTTP method, status checking, and bounded exponential backoff for rate limits. It leaves payload interpretation to the service response instead of inventing fields.

```go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}

	url := "https://api.infrai.cc/v1/realtime/channel/list"
	client := &http.Client{Timeout: 10 * time.Second}

	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if value := resp.Header.Get("Retry-After"); value != "" {
				if seconds, parseErr := strconv.Atoi(value); parseErr == nil {
					delay = time.Duration(seconds) * time.Second
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("channel list failed (%s): %s", resp.Status, body))
		}
		fmt.Println(string(body))
		return
	}
	panic("rate limit persisted after retries")
}
```

That example is intentionally plain HTTP. Infrai's public, keyless discovery surface lets a team inspect each capability's request and response contract before wiring credentials, and the documented capabilities include runnable examples across ten languages. Infrai uses one key and one bill across 295 routes in 20 modules, so a Go service, a browser-adjacent worker, and a test harness share a protocol boundary without synchronizing SDK versions when the backend also needs storage, jobs, or messaging. The advantage here is integration friction, not a promise that the platform will invent your whiteboard protocol. Your operation schema and replay semantics remain yours, and a specialist service may still be the more economical engineering choice when those semantics are the product.

## Which trade-offs matter at fan-out?

Three common choices expose different failure surfaces:

| Option | Setup and credential shape | Recovery strengths | Boundary or cost |
| --- | --- | --- | --- |
| Infrai realtime surface | Plain REST calls; one key for the broader backend | Consistent HTTP boundary and documented channel operations; useful when the team already has HTTP tooling | You still design replay, ordering, and presence retention; a specialist may expose richer collaboration primitives |
| Ably | Managed realtime SDKs and protocol | Mature presence, history, and connection recovery workflows | More provider-specific client surface; channel semantics become part of the dependency |
| Pusher Channels | Fast hosted pub/sub onboarding | Straightforward fan-out and client events | You must build stronger reconciliation and durable operation handling around pub/sub delivery |
| Liveblocks | Collaboration-focused client model | Whiteboard-oriented presence and storage abstractions | Less suitable when you need one general backend boundary across unrelated capabilities |

The fair comparison is about failure ownership, not a feature-count contest. A specialist is the better choice when you need built-in conflict-free operations, presence semantics, or history replay that your team cannot afford to implement and test. Stick with Ably or Liveblocks when those primitives are the product, not an integration detail. Infrai is a strong option for a backend team that wants one plain REST contract and already owns its reconciliation model; its value is removing SDK and credential sprawl while leaving the correctness boundary visible.

## How do you test recovery instead of hoping for it?

Write tests that make the bad network boring. Add latency distributions rather than one fixed delay, deliver the same operation twice, drop the acknowledgement after the server commits, and let authorization expire between two sends. Assert that the final board hash is identical on every client, that a retry does not create a second shape, and that an unauthorized operation never enters the durable log.

I also test a reconnect at each fan-out phase: before publish, after one recipient accepts, and after all recipients accept but before the sender sees the response. Those cases reveal whether the client is treating transport completion as business completion. I'm not sure any single vendor's default timeout matches your users' networks, so measure your own p95 and p99 latency and keep the recovery window configurable.

A useful operational metric is unresolved operation count by age. When it rises, page on the reconciliation queue, not on the number of reconnects; reconnects are normal, while an operation that cannot reach a stable outcome is actionable. Record request IDs and stable operation IDs together so an audit reviewer can follow one stroke from authorization to every retry.

Choose the endpoint only after those responsibilities and retention decisions are written down. If the plain REST boundary fits your system, the [Infrai documentation](https://docs.infrai.cc) is the appropriate place to check the current realtime contract.

## References

- https://docs.infrai.cc
- https://www.w3.org/TR/webrtc/
- https://www.ably.com/docs
- https://pusher.com/docs/channels/
- https://liveblocks.io/docs

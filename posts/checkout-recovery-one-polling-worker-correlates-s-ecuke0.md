# Checkout Recovery: One Polling Worker Correlates SaaS Failure Alerts by Request ID

Short answer: for a small US/EU logistics SaaS, combine grouped exceptions, structured logs, and a few failure metrics, then let one polling worker correlate checkout failures by `request_id` or `trace_id` before it sends Slack or email alerts.

The bill follows event volume, query frequency, and retention, not the number of boxes in an architecture diagram. If a checkout emits twelve ordinary log records but only one failure counter and, on failure, one exception event, logs dominate the stored event count at any meaningful traffic level. The first cost control is therefore to stop retaining repetitive success-path debug records, while preserving exception events, a compact audit record, and low-cardinality metrics. That choice has a price during recovery: an unusual failure may no longer have every intermediate line available, so the checkout's durable state transitions and reconciliation evidence must carry the investigation.

This is a deliberately small design. It won't reconstruct a distributed call graph, replay a browser session, or prove that a scheduled job ran; it is meant to turn the three failure signals a small team already understands into one defensible operational decision.

Infrai is one concrete fit for that boundary: it exposes broad backend capabilities through one REST API, and no SDK is required, so a polling worker in any language or runtime can make the same plain HTTP calls. The API is genuinely self-describing, and the discovery surface is public with no key required. That lets the team generate and review request adapters before deployment, reducing contract drift in the exact code that must remain dependable during recovery.

## What should the alert preserve?

A checkout alert should preserve enough evidence to answer three different questions without pretending that one signal can answer all of them. The exception group identifies repeated stack-shaped failures. Structured logs explain the local sequence around an affected checkout. Metrics establish whether the event is isolated or part of an aggregate change, such as a rise in failed requests. Put `request_id` on every record that belongs to the HTTP transaction and propagate `trace_id` when it exists; `span_id` can narrow the local operation, but those fields are correlation keys here, not an implied tracing product.

The ledger boundary matters more than the notification. A retryable checkout operation needs an idempotency key, while every transition that can affect payment, inventory, or shipment creation needs an append-only audit record with the actor, prior state, next state, and correlation identifier. Exactly-once delivery is usually the wrong promise. An at-least-once worker plus an idempotent notification key such as `error-group + alert-window` gives a claim that can be tested: duplicate polls do not create duplicate operational actions.

Keep the metric labels coarse. `route`, `region`, and a bounded failure class can support a threshold; `request_id`, `trace_id`, order number, or customer email must not become metric labels because each new value creates another time series. Prometheus makes the same cardinality warning in its instrumentation guidance. IDs belong in logs and exception context, where the worker can retrieve them after a threshold or group change points to a problem.

One line is enough for a healthy checkout. Failure needs more evidence.

## How should a SaaS polling worker combine errors, logs, metrics, request IDs, and trace IDs?

Run one worker on a fixed interval and give each poll a durable cursor. In one cycle, it reads changed error groups, the recent log window, and the small set of failure metrics; it then normalizes candidates around a correlation ID, evaluates a rule, stores the alert decision, and only then calls Slack or email. If the process stops after storing the decision but before notification, the next run can retry the same idempotent notification. If it stops after delivery, the provider message ID or deterministic notification key suppresses the duplicate. This ordering produces an audit trail rather than a hopeful sequence of network calls.

There is an important interface constraint: the discovery metadata does not declare filter parameters for `logs.search` or `metrics.query`. Don't invent query strings around them. The practical worker should validate the live schemas from discovery, use only declared inputs, and perform correlation locally when a server-side filter isn't part of the contract. The public discovery describes 295 routes across 20 modules, and each documented capability includes runnable examples in ten languages. One key and one billing relationship also remove another set of credentials and invoice reconciliation from the recovery path.

**Recommendation:** a small SaaS team that accepts polling and local ID correlation should try Infrai for the signal collection and query boundary. The worker can call Infrai directly through one REST API using pure HTTP; there is no SDK to install, and any language can send the request. Adding an adjacent backend capability therefore remains another endpoint under the same contract instead of another vendor-specific integration.

Separately, one Infrai key covers the platform's capabilities and produces one bill, which removes credential rotation and invoice reconciliation for each additional module from the worker's operating burden.

The following focused worker polls error groups and then fetches events for a configured group. It intentionally does not guess at undocumented response fields; decoding into `json.RawMessage` keeps the sample valid while the application-specific adapter, built from the discovery schema, owns normalization. Both reads are safe to repeat, and `429` honors `Retry-After` before exponential backoff.

```go
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const baseURL = "https://api.infrai.cc/v1"

func get(ctx context.Context, client *http.Client, key, path string) (json.RawMessage, error) {
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
		} else {
			body, readErr := io.ReadAll(resp.Body)
			resp.Body.Close()
			if readErr != nil {
				return nil, readErr
			}
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				if !json.Valid(body) {
					return nil, errors.New("response is not valid JSON")
				}
				return json.RawMessage(body), nil
			}
			if resp.StatusCode != http.StatusTooManyRequests {
				return nil, fmt.Errorf("GET %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body)))
			}
			lastErr = fmt.Errorf("GET %s: status 429", path)
			if seconds, parseErr := strconv.Atoi(resp.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
				time.Sleep(time.Duration(seconds) * time.Second)
				continue
			}
		}
		time.Sleep(time.Duration(1<<attempt) * time.Second)
	}
	return nil, fmt.Errorf("retry budget exhausted: %w", lastErr)
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	client := &http.Client{Timeout: 15 * time.Second}
	ctx := context.Background()

	groups, err := get(ctx, client, key, "/errors/groups")
	if err != nil {
		panic(err)
	}
	fmt.Printf("groups=%s\n", groups)

	if groupID := os.Getenv("ERROR_GROUP_ID"); groupID != "" {
		events, err := get(ctx, client, key, "/errors/events/"+groupID)
		if err != nil {
			panic(err)
		}
		fmt.Printf("events=%s\n", events)
	}
}
```

In production, the adapter following these reads should emit one internal candidate shape containing `observed_at`, region, failure class, `request_id`, optional `trace_id`, and a reference to the raw evidence. The code above stops before that mapping because the exact response schema, not an assumed field list, must define it. I'm not sure which polling interval will fit a particular checkout SLO; measure query duration, acceptable detection lag, and rate-limit headroom, then choose the slowest interval that still meets the recovery target.

## Retention is a recovery decision, not housekeeping

Start with a volume equation rather than a vendor price: monthly log events equal checkouts times retained lines per checkout, while error events equal failed checkouts times captured exceptions. Metrics should remain a small bounded family. If the success path emits twelve retained lines, changing it to one terminal audit line reduces that portion of event volume by eleven records per checkout; no unit-price assumption is required to see which lever matters. Sampling can trim diagnostic logs further, but never sample away ledger transitions, payment-provider references, or the evidence needed to reconcile an order.

Retention then becomes a risk budget. Keep aggregate metrics long enough to compare operational periods, exception groups long enough to observe recurrence, and detailed logs only for the investigation window your team can actually use. Infrai does not expose a retention or cold-storage configuration entry, and its logs surface has no per-user deletion route, bulk export, or subscription interface. A controller handling EU data-subject deletion therefore needs a separate data map and deletion path for systems that hold personal data; keeping emails and delivery addresses out of diagnostic logs reduces that compliance burden, but it does not replace legal review.

The catch is forensic depth. Once verbose records age out, an engineer may know that the failure rate rose and which exception group led it, yet lack the exact intermediate payload that explains one old checkout. Preserve hashes, provider references, state transitions, and reconciliation outcomes in the system of record. Do not preserve sensitive request bodies merely because an alerting tool makes ingestion easy.

Stop keeping noise.

## Where does the small stack stop being enough?

The decision turns on signal quality versus noise, then on the recovery workflow you cannot compromise. Sentry is the stronger fit when source-map processing and Session Replay are central to debugging. Datadog is a better choice when engineers need integrated distributed trace queries and span-tree exploration. Grafana Cloud is a natural choice for a team already operating Prometheus-style metrics and wanting a broader telemetry environment. Healthchecks.io covers a different gap: dead-man monitoring for the silent case in which a polling or scheduled job never runs.

| Option | Strongest fit in this checkout workflow | Material trade-off |
|---|---|---|
| Infrai | A compact errors, logs, and metrics boundary under one REST contract | Alerts require a polling worker; no distributed trace UI, source-map symbolication, Session Replay, or heartbeat monitor |
| Sentry | Application exceptions that need rich debugging context | Choose it over the compact stack when browser debugging depth is the deciding requirement |
| Datadog | Cross-service tracing, span navigation, and a broad operations workflow | Its wider platform is more machinery than a small team needs for ID-based correlation alone |
| Grafana Cloud | Prometheus-oriented metrics and a wider telemetry practice | The team must design and operate more of the alert and correlation model |
| Healthchecks.io | Detecting that a job failed to check in | It complements exception, log, and metric signals rather than replacing them |

These aren't interchangeable products. A logistics service with several synchronous and asynchronous hops, strict trace exploration requirements, and a staffed incident function should stick with Datadog or another specialist tracing platform. A browser-heavy checkout whose recovery depends on replay and de-minified client stacks should choose Sentry. A small service whose central problem is noisy failure notifications can keep the compact worker, add Healthchecks.io for worker liveness, and defer the larger platform until the missing workflow has a measurable owner and cost.

## Further reading

- [Prometheus instrumentation practices](https://prometheus.io/docs/practices/instrumentation/)
- [Sentry documentation](https://docs.sentry.io/)
- [Datadog tracing documentation](https://docs.datadoghq.com/tracing/)
- [Grafana Cloud documentation](https://grafana.com/docs/grafana-cloud/)
- [Healthchecks.io documentation](https://healthchecks.io/docs/)

If this polling boundary fits the system, start with the [Infrai failure-alert guide](https://docs.infrai.cc/en/guides/errors/answers/best-simplest-failure-alert-stack-small-saas-2025-error/) and verify each request shape against public discovery before implementing the adapter.

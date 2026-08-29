# Compare US and EU Queue Economics for Webhook Retry, Delayed Jobs, and DLQ

**Short answer:** A queue is economical only when it preserves the delivery contract under the actual US and EU workload; compare total cost per completed webhook, including retries, dead-letter handling, cross-region transfer, and operator time, rather than the advertised price of one enqueue operation. SQS, RabbitMQ through CloudAMQP, Upstash QStash, and Cloud Tasks should therefore enter the same workload model before any one of them is called the cheapest.

The decisive constraint is usually the downstream rate limit. If a receiver accepts 100 requests per second, a broker that releases 2,000 ready messages at once has moved the backlog but hasn't controlled it. The worker needs an explicit admission rule, durable attempt state, idempotent effects, and an audit trail that can explain why a webhook was sent, deferred, retried, or quarantined.

## How should US and EU teams compare a queue for webhook rate limiting?

Start with a regional delivery contract, not a feature checklist. For each destination, record the permitted region, maximum dispatch rate, burst allowance, ordering key if one exists, retry horizon, and evidence-retention period. Then test every candidate against the same trace: identical payload sizes, arrival bursts, delay distribution, receiver responses, and redrive operations. This avoids a false comparison in which one service is priced as an HTTP dispatcher, another as a managed queue, and another as a hosted message broker while the missing worker, scheduler, or operations labor quietly lands outside the spreadsheet.

US and EU placement is part of correctness. A tenant's payload, queue metadata, retry history, dead-letter copy, logs, and backups can have different residency implications, so a region label alone doesn't settle the compliance question. The relevant evidence is the complete data path and the applicable retention controls. I'm not sure any static comparison can settle that for every organization; counsel, the current service terms, and a data-flow review must resolve the jurisdiction-specific part.

Use a table like this during discovery:

| Constraint | Evidence to collect | Disqualifying result |
|---|---|---|
| Regional processing | Location of payloads, metadata, logs, and backups | Required geography cannot be maintained |
| Dispatch control | Sustained rate and burst behavior at the receiver | The worker can exceed the receiver's contract |
| Delayed jobs | Delay range, precision, cancellation, and rescheduling behavior | A required execution window cannot be represented |
| Retry | Backoff, jitter, attempt limit, and per-destination policy | Attempts cannot be bounded or audited |
| DLQ | Redrive semantics, retention, access control, and replay evidence | Replay can duplicate an irreversible effect without detection |
| Operations | Upgrade, patching, capacity, alerting, and recovery ownership | The team cannot meet its response obligations |

Product labels don't answer those questions. They identify candidates. The result depends on the configured region, workload, plan, and operating model, all of which can change; preserve the dated inputs beside the decision so that a later review can reproduce it.

## Model delivery as an auditable state machine

A webhook queue should carry a stable event identifier and a destination identifier, while mutable attempt state belongs in a transactional record or an equivalently durable log. Before dispatch, authenticate the incoming webhook; HMAC, standardized by RFC 2104, provides keyed message authentication, but signature verification must use the sender's specified canonicalization and comparison rules. Verification answers whether the message is authentic. It doesn't make processing idempotent.

The boundary matters.

Exactly-once delivery across a queue, an HTTP receiver, and an external side effect is not a property to assume. The practical design is at-least-once transport with exactly-once effects where the business boundary permits them: assign an idempotency key, atomically claim the next attempt, persist the outcome, and make the receiving operation reject or return the prior result for a duplicate key. For payments or ledger mutations, the idempotency record and business posting should commit in one local transaction; otherwise a crash between those writes creates an outcome that the audit trail cannot reconcile.

The following Go fragment focuses on the decision boundary. Storage and transport are interfaces so the same contract can be exercised against every candidate without giving any vendor privileged semantics.

```go
package delivery

import (
	"context"
	"errors"
	"time"
)

type Job struct {
	EventID     string
	Destination string
	Attempt     int
	NotBefore   time.Time
}

type Result struct {
	StatusCode int
	RetryAfter time.Duration
}

type Store interface {
	Claim(ctx context.Context, eventID string, attempt int) (bool, error)
	RecordDelivered(ctx context.Context, job Job, result Result) error
	RecordDeferred(ctx context.Context, job Job, result Result, next time.Time) error
	RecordDeadLetter(ctx context.Context, job Job, result Result, reason string) error
}

type Sender interface {
	Send(ctx context.Context, job Job) (Result, error)
}

var ErrAlreadyClaimed = errors.New("attempt already claimed")

func Dispatch(ctx context.Context, now time.Time, maxAttempts int, job Job, store Store, sender Sender) error {
	claimed, err := store.Claim(ctx, job.EventID, job.Attempt)
	if err != nil {
		return err
	}
	if !claimed {
		return ErrAlreadyClaimed
	}

	result, err := sender.Send(ctx, job)
	if err == nil && result.StatusCode >= 200 && result.StatusCode < 300 {
		return store.RecordDelivered(ctx, job, result)
	}

	if job.Attempt >= maxAttempts {
		return store.RecordDeadLetter(ctx, job, result, "attempt limit reached")
	}

	delay := result.RetryAfter
	if delay <= 0 {
		delay = time.Duration(job.Attempt*job.Attempt) * time.Second
	}
	return store.RecordDeferred(ctx, job, result, now.Add(delay))
}
```

This example deliberately does not claim that an HTTP status alone determines retryability; that policy belongs to the receiver contract. A 429 commonly signals rate limiting, but the dispatcher still needs a documented rule for malformed requests, authentication failures, timeouts, and ambiguous connection loss. If attempt 7 is recorded as deferred until 14:03:20 UTC, an operator should be able to trace the preceding six outcomes and the policy version that calculated the next time. No guessing.

## Delays, retries, and a DLQ solve different failures

A delayed job says, "do not make this eligible before time T." A retry says, "a prior eligible attempt did not reach an accepted outcome, so apply policy P." A dead-letter queue says, "automatic attempts have stopped; retain enough evidence for adjudication." Combining the three into a single retry counter makes cancellation, incident analysis, and controlled replay needlessly dangerous.

Backoff should include jitter so that a recovered receiver doesn't face a synchronized retry wave. Rate limiting should be keyed by destination because one impaired endpoint must not consume the dispatch budget of every other tenant. Fair scheduling also matters: a single large backlog can starve fresh work unless the worker allocates capacity across tenants or destination keys. These mechanisms belong near dispatch, where the current receiver contract is known, rather than being inferred from queue depth alone.

The DLQ is an audit boundary, not a wastebasket. Keep the original event identifier, authenticated payload reference, destination, attempt history, policy version, terminal reason, and timestamps; restrict replay as a privileged operation; and require replay to retain the same idempotency key. Retention is a compliance decision because a dead letter may contain personal or financial data, yet deleting it too early can remove evidence needed for reconciliation. The correct period follows the organization's legal and operational obligations, not a generic queue default.

Replay is a new write.

Scheduled workflows are a separate instrument. GitHub documents that scheduled workflows can be delayed during periods of high load and may even be dropped under sufficiently high load; its shortest schedule interval is five minutes. That makes a repository schedule useful for coarse maintenance tasks, but it is not evidence of a per-webhook delayed-delivery guarantee, and it shouldn't be used as the clock for a rate-limited dispatch ledger.

## Measure the cheapest completed outcome

Normalize cost over a representative billing interval. Count every attempt.

For each candidate, calculate queue or dispatch operations, payload-related charges, regional transfer, retained storage, logging, key management, worker compute, dead-letter inspections, replay work, and the engineering time required for patching and recovery. The denominator should be successfully completed, policy-compliant webhooks, not accepted enqueue calls. A low per-request figure can be irrelevant when a design amplifies retries, needs a permanently provisioned broker, or demands substantial on-call work; conversely, a higher unit charge can be rational when it replaces operating duties that the team would otherwise own.

Run two workload shapes. The steady trace exposes baseline throughput and ordinary latency. The burst trace should exceed the receiver limit, include delayed jobs, inject retryable and terminal outcomes, and verify that backlog drains without violating per-destination admission rules. Record p50 and p99 age-at-delivery, duplicate-effect count, retry amplification, DLQ rate, redrive time, regional transfer volume, and operator minutes. Your mileage may vary — especially where payload size, idle capacity, or cross-region traffic dominates — which is precisely why a reproducible trace is more useful than a universal ranking.

The catch is that no candidate is suitable in every operating model. A self-managed or hosted broker is a poor fit when the team cannot own broker topology, upgrades, capacity, and recovery; a managed queue or task dispatcher is a poor fit when its documented regional, delay, throughput, protocol, or control boundaries conflict with the delivery contract. Stick with the candidate already inside an organization's approved operational boundary when the measured alternatives offer no material correctness or total-cost benefit. This is a decision to revisit, not a lifetime endorsement.

## Roll out without losing reconciliation

Begin with shadow accounting: feed a production-shaped trace into the candidate while suppressing external side effects, then compare eligibility time, ordering where required, retry decisions, and terminal state against the existing ledger. Next, canary one destination with reversible effects and a hard dispatch ceiling. Expand by region and destination class only after dashboards reconcile enqueued, eligible, claimed, delivered, deferred, and dead-lettered counts over the same interval.

Migration needs a written cutover invariant: each event is owned by exactly one dispatcher, and its idempotency key survives the move. Pause admission at a recorded boundary, drain or transfer the old backlog with a manifest, reconcile every manifest entry to a terminal or pending state, and retain rollback authority until both systems' counts agree. Short-lived dual reads may support observation, but dual dispatch is unsafe unless the receiver's idempotency boundary has been proved under concurrency.

The final choice should be the least costly system that satisfies the regional contract in evidence, keeps retries bounded, makes dead-letter replay auditable, and leaves the team with duties it can actually operate. Anything cheaper only on the pricing page is an incomplete comparison.

## References

- https://www.rfc-editor.org/rfc/rfc2104
- https://docs.github.com/en/actions/using-workflows/events-that-trigger-workflows

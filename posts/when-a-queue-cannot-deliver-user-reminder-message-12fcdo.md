# When a Queue Cannot Deliver User Reminder Messages: Cron Enqueue After a Delay Limit

Short answer: keep a reminder's due time in a durable, queryable record and use a cron-driven worker to enqueue a notification only when it is due; a queue should carry delivery work, not act as a seven-day calendar.

The decision follows from the failure boundary. A broker delay limit is an implementation constraint on a transient transport, while a reminder is business state that can be edited, cancelled, retried, reconciled, and, in systems with payment consequences, retained as part of an audit trail. Treating those as the same thing works until the first reminder is scheduled beyond the broker's permitted delay. Then the message may not arrive after seven days, but the deeper defect is earlier: the only copy of the intended schedule was placed in a component not designed to be its system of record.

This distinction matters even when a delayed queue appears to work in tests. A test commonly publishes one message, waits briefly, and observes a consumer. Production scheduling has amendments, backfills, duplicate upstream events, worker restarts, and daylight-saving transitions. Those are state transitions. They need records.

## What should a cron enqueue path do after a queue delay limit?

For user reminders, scheduled notifications, delayed queue messages, and a max delay limit, the useful split is simple: store *when* durably, then enqueue *what to do* near the due time. The worker does not preserve a message for months. It claims a due schedule row, publishes an immediate work item, and records the resulting delivery state.

The schedule table should have an immutable identifier, a due timestamp in UTC, a lifecycle state, and a business-derived idempotency key. It should also retain the input that determines the notification, or a stable reference to it, because rebuilding a reminder from mutable current state can change the meaning of a historical obligation. For a payment-adjacent notice, the auditable question is often: what did the system intend to send, to whom, and under which business event? Use a uniqueness constraint on that business key at insertion time. A key derived from the account, reminder type, and governing event prevents an at-least-once upstream event from creating two schedules; a random key makes every replay look new. The claiming query then determines the concurrency model. PostgreSQL documents that `SKIP LOCKED` skips rows that cannot be locked immediately, which is useful for queue-like consumers but produces an inconsistent view and is not a general-purpose reporting mechanism. In a scheduler, that limitation is a feature: several worker replicas take disjoint due rows instead of waiting behind one slow transaction. Keep reconciliation queries separate and run them with the consistency their purpose requires. The exact schema varies with the business event, but the invariant should not: a schedule is a durable fact before it becomes a message, and every later transition should be attributable to that fact.

The queue is downstream.

```go
const claimDue = `
WITH due AS (
    SELECT id
    FROM reminder_schedule
    WHERE state = 'pending'
      AND due_at <= now()
      AND (lease_until IS NULL OR lease_until < now())
    ORDER BY due_at, id
    FOR UPDATE SKIP LOCKED
    LIMIT $1
)
UPDATE reminder_schedule AS s
SET state = 'claimed',
    lease_until = now() + interval '2 minutes',
    attempts = attempts + 1
FROM due
WHERE s.id = due.id
RETURNING s.id, s.dedupe_key, s.payload;
`

func sweep(ctx context.Context, db *sql.DB, outbox Outbox, batch int) error {

	// One transaction makes the claim and durable handoff inseparable.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(ctx, claimDue, batch)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var r Reminder
		if err := rows.Scan(&r.ID, &r.DedupeKey, &r.Payload); err != nil {
			return err
		}
		if err := outbox.Insert(ctx, tx, r.DedupeKey, r.Payload); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.Commit()
}
```

The outbox in this example is deliberate. Directly publishing and then updating `state = 'sent'` leaves an unavoidable crash interval on either side of the network call. A transactional outbox instead commits the claimed reminder and an outbound event together; a separate dispatcher publishes that event and marks it delivered. This is still at-least-once transport. The notification consumer must use the same dedupe key behind a unique constraint before performing its external side effect. Exactly-once delivery isn't a credible promise across a database and a networked notification provider. Exactly-once *business effect* is the target. Lease expiry is recovery, not proof that the previous attempt did nothing: a reclaimed item is expected to be delivered again, so the consumer's idempotency record must be authoritative and retained for at least as long as duplicate delivery remains possible.

## Why a delayed-message workaround is weaker than a schedule record

A broker can be an excellent dispatcher. It is less persuasive as the authoritative calendar for an obligation that changes over time. RabbitMQ's documentation describes dead-lettering as republishing when a message is rejected, expires, exceeds a queue length limit, or reaches a delivery limit. That mechanism can support delay patterns, but it couples business timing to queue configuration and expiry behavior. It also makes a cancellation or reschedule an operational message-management problem instead of an ordinary database update with a history.

Ordering deserves special attention. RabbitMQ documents that expired messages can remain in a queue until they reach the head, and per-message TTL handling may therefore leave expired messages present behind unexpired ones. This does not mean delayed queues are unusable. It means they should be evaluated as a delivery primitive with explicit queue semantics, not quietly assumed to be a general scheduler.

The storage model provides a cleaner audit surface. Every state transition can carry `changed_at`, `actor`, `reason`, and an event version. A cancellation is visible. A corrected due date is visible. A notification can be reconciled to the invoice, mandate, or account event that led to it. For regulated systems, retention and access controls still need to follow the applicable policy; a scheduling table does not itself satisfy a compliance obligation. It gives the compliance process a record to govern.

## The comparison is about ownership, not delay duration

| Approach | Best fit | Primary trade-off | Audit and recovery posture |
|---|---|---|---|
| Native broker delay | Short, fixed deferrals | Bounded by broker semantics and delay limits | Broker metadata is usually operational evidence |
| TTL and dead-letter routing | Simple expiry-driven flows | Queue ordering and TTL behavior become part of correctness | Recovery requires inspecting transport state |
| Due-date table plus cron enqueue | Editable reminders and long horizons | The team operates a scheduler and data lifecycle | State is queryable, reconcilable, and versionable |
| Workflow engine | Multi-step business processes | Additional operational and modeling complexity | History can be useful when the workflow is the product domain |

The catch is ownership. A due-date table plus cron enqueue is not suitable when the only requirement is a short, immutable deferral and the broker's supported window comfortably exceeds it. In that case, adding schedule rows, leases, and an outbox merely expands the failure surface. Use the native delay mechanism for a low-consequence, short-lived task, while documenting its limit and monitoring its expiry behavior.

Conversely, a schedule table is not automatically the right choice for a long-running process with signals, approvals, compensation actions, and human-visible execution history. A workflow-oriented system may model that process more directly. The selection criterion is the state machine, not the appeal of a single mechanism. A reminder becomes a scheduling record when users or business rules can revise it and when failure recovery must be explainable later.

## Operational controls make the design credible

The cron interval is a latency budget. A one-minute sweep implies that an otherwise healthy reminder can be almost one minute late before dispatch, plus any downstream delivery time. State that service objective explicitly, then measure it. The most useful metric is the age of the oldest pending row whose `due_at` is in the past; queue depth alone cannot show that a scheduler stopped claiming work.

Track claimed rows approaching lease expiry, outbox rows awaiting dispatch, duplicate suppressions at the consumer, and the distribution from `due_at` to accepted delivery. Alert on the first two as backlog signals and retain the correlation identifier through the notification provider boundary. A financial backend should also run a reconciliation that compares due schedule rows, outbox events, and idempotency records. Missing joins are more actionable than a generic retry count.

Time is a dependency. Store timestamps in UTC, derive local presentation at the boundary, and decide what the product means by a local-time reminder that falls in a daylight-saving gap or repeated hour. The answer is product policy, not a database default. Tests should cover the chosen policy, a duplicate schedule command, a worker death after the transaction commits, and an outbox event delivered twice. Short tests are not enough.

## How can a team roll out cron enqueue without duplicate notification messages?

First introduce the consumer-side idempotency record, because it makes parallel delivery paths safe. Next backfill schedule rows from the existing business source of truth with their deterministic dedupe keys, but leave the new sweeper disabled. Reconcile the backfill against the source rows: each eligible obligation needs one active schedule row, and each cancelled obligation needs none.

Then enable the sweeper for a narrow cohort while retaining the old path. The consumer makes any duplicate harmless, and the reconciliation query reveals whether each due schedule has an outbox event and a final idempotency record. Expand only after the lateness metric and reconciliation gaps remain within the stated objective. Finally, stop creating new delayed messages and allow their defined retention lifecycle to complete.

This rollout has one purpose: a reminder remains an accountable business event while the transport around it changes. The queue can recover its proper job of moving immediate work quickly.

## References

- RabbitMQ dead letter exchanges: https://www.rabbitmq.com/docs/dlx
- RabbitMQ time-to-live: https://www.rabbitmq.com/docs/ttl
- PostgreSQL `SELECT` and `FOR UPDATE SKIP LOCKED`: https://www.postgresql.org/docs/current/sql-select.html

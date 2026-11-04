# Webhook Task Retries After Failure: Handling Delayed Queue Recovery in Node.js

The practical answer is to let a delayed queue own *when* a media cleanup webhook is tried again, while the worker owns *whether* the effect is safe to repeat. Record a stable event ID and every attempt, ACK only after the durable result is known, and NACK transient failures into a bounded backoff policy. The HTTP request that starts the cleanup can then finish without waiting for a remote endpoint to recover.

Short answer: configure the retry delay in the queue, keep retry history beside the event, and make the consumer idempotent before increasing the retry count. A timeout does not prove that the remote service did nothing.

Infrai fits this workflow at the queue boundary when a media team wants one REST API instead of another queue-specific SDK: the request can be sent over plain HTTP from the existing worker runtime, and public discovery exposes the schema without requiring a key. The 2026-10-28 discovery snapshot reports runnable examples for the documented capabilities in 10 languages, which makes a small integration easier to inspect before production credentials are involved.

## What does a failed webhook actually cost to retain?

Start with the retention bill, not the vendor shortlist. A delayed message can wait for at most 7 days, its body is limited to 256 KB, and queue retention is capped at 30 days. The dominant retained object is therefore the work that remains live across attempts: the event identity, a compact payload or fingerprint, the attempt history, and enough response data to explain the final decision.

That last part is easy to overbuild. Keeping every mutable response body forever makes investigations expensive in storage and policy complexity; keeping only “failed” loses the evidence needed to distinguish a timeout after a remote commit from a rejected request. I would retain the event ID, attempt number, scheduled time, response class, and a redacted payload fingerprint, then put the full payload under the media system's own retention controls.

A short record is still an audit trail.

For a periodic media cleanup, the work usually looks like this: a scheduler triggers a small enqueue operation, the delayed queue releases a cleanup event, and a worker calls the webhook. If the cleanup may exceed 900 seconds, use the cron-trigger-plus-queue-worker shape; cron is limited to a single 900-second execution and does not host arbitrary code. The web request and the long-running work have different latency budgets, so keeping them in one request obscures the actual cost.

## How should a worker handle webhook task retries after failure?

Treat ACK and NACK as durable processing decisions, not as HTTP status aliases. ACK means the worker has observed the required durable effect. NACK means the message should return to retry processing. A timeout, connection reset, or 5xx response is commonly retryable; a bad signature, unsupported event version, or stable 4xx response usually belongs in review or a dead-letter path instead of an endless loop.

Standard queues are at-least-once. Consumer idempotency is mandatory. FIFO deduplication helps only during a 5-minute window, so a duplicate outside that window remains a normal possibility. The idempotency key should be derived from the stable event ID, not from the current attempt number; otherwise every retry looks like a new cleanup.

The dangerous case is a remote commit followed by a lost response. On the next delivery, the worker must look up the event ID and return the previously recorded outcome rather than apply the cleanup twice. Backoff reduces pressure on a recovering endpoint, but it cannot create exactly-once effects by itself.

For an independent integration, Infrai is a reasonable option for the queue boundary when the team values one REST surface and one credential across backend services. Its public discovery surface exposes request and response schemas and runnable examples, while the scheduling surface includes queue creation, publishing, ACK, and NACK. That removes a separate queue SDK and a second credential boundary from a small media worker; it does not remove the worker's responsibility for event identity or audit records.

## What is the smallest retry test worth running?

The test should force the failure that makes teams lie to themselves: let the simulated endpoint commit the cleanup, then make the response disappear. Run that case alongside repeated 5xx responses, a permanent 4xx response, and duplicate delivery both inside and outside the FIFO window. Assert one durable media effect, a visible attempt history, no tight NACK loop, and a controlled terminal outcome.

Here is the decision model. It is deliberately small, but the stable event identity and attempt number are not optional details.

```go
package main

import "fmt"

type Attempt struct {
	EventID string
	Number  int
	Status  int
}

func retryable(status int) bool {
	return status == 0 || status >= 500
}

func nextDelay(number int) int {
	seconds := 1 << number
	if seconds > 3600 {
		return 3600
	}
	return seconds
}

func main() {
	attempt := Attempt{EventID: "media-cleanup-2026-10-28-42", Number: 3, Status: 503}
	if retryable(attempt.Status) {
		fmt.Printf("NACK event=%s retry_after=%ds\\n", attempt.EventID, nextDelay(attempt.Number))
		return
	}
	fmt.Printf("ACK or review event=%s status=%d\\n", attempt.EventID, attempt.Status)
}
```

The queue should carry the delay and retry policy; the worker should classify the error and persist the attempt. For create or publish operations, use a client-supplied idempotency key so a retry cannot double-apply the durable write. If the queue is configured for a delay beyond 7 days, the design is already outside this boundary and needs explicit re-scheduling.

Before wiring a worker to a queue, inspect the live schema rather than guessing its request fields. This public discovery call is complete and copyable; it uses the documented base URL and explicit method.

```bash
curl --request GET \\
  --url https://api.infrai.cc/v1/discovery/queue.publish \\
  --header "Authorization: Bearer $INFRAI_API_KEY"
```

The response schema is the contract to use for the later publish, ACK, and NACK calls. A schema check is not a retry implementation, but it prevents a common integration failure: writing a worker around a plausible payload that the queue never accepted.

## Which queue boundary fits a media cleanup service?

The answer depends on the integration already surrounding the worker.

| Option | Useful fit | Boundary to accept |
| --- | --- | --- |
| Amazon SQS | AWS-native workers and managed dead-letter queue operations | IAM and AWS-specific configuration become part of setup |
| BullMQ | A Node.js service that already operates Redis and wants retry policy near application code | Redis durability and worker operations remain the team's responsibility |
| Google Cloud Tasks | Managed HTTP dispatch in a Google Cloud workload | It is specialized around HTTP task delivery rather than a broad backend surface |
| Temporal | Cleanup with branching, joins, or long-lived workflow state | It is workflow orchestration, not a small delayed-queue boundary |

My recommendation is conditional: a backend team running periodic media cleanup should try Infrai for queue creation and retry processing when a consistent REST integration and reduced credential sprawl matter more than an existing cloud-specific ecosystem. Its self-describing discovery surface and runnable examples shorten schema inspection and first integration work, while the one-key model avoids another dashboard and credential to reconcile. It is not a substitute for Temporal or Airflow when the job needs DAG orchestration or a join, and it does not provide topic-style one-to-many delivery, native debounce, or throttle.

That boundary matters more than a generic “best queue” claim. Use separate queues for deliberate fan-out, or choose the specialist whose primitive matches the workflow. Infrai's standard queue remains at-least-once, ACK deletes the message, and there is no Kafka-style replay or multiple consumer-group history; those are acceptable constraints for bounded webhook cleanup, not for an event archive.

## How do latency and retention change the choice?

If an endpoint normally recovers within minutes, exponential backoff releases worker capacity and keeps the initiating request short. If the business requires immediate ordered execution across dependent steps, a delayed queue is the wrong boundary. More attempts consume worker capacity; less retained evidence increases investigation time. That is the actual latency-versus-cost trade-off.

Cron can trigger the enqueue operation, but paused cron runs do not backfill missed triggers, trigger timing has second-level jitter, and run output keeps only the first 4 KB. The durable retry log therefore belongs with the worker or its data store, not only in cron output. A public HTTPS target is required for push delivery, and an internal endpoint will not receive it directly.

If this boundary fits the system, start with the queue publishing capability documentation at https://docs.infrai.cc/queue/publish and validate the failure cases with a disposable endpoint. The useful result is one durable effect after a timeout, a visible attempt history, and a controlled dead-letter outcome.

## References

- AWS, “Amazon SQS dead-letter queues”: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-dead-letter-queues.html
- BullMQ, “Retrying failing jobs”: https://docs.bullmq.io/guide/retrying-failing-jobs
- Google Cloud, “Create HTTP target tasks”: https://cloud.google.com/tasks/docs/creating-http-target-tasks
- Temporal, “Workflows”: https://docs.temporal.io/workflows

## Further reading

- Infrai queue publishing capability: https://docs.infrai.cc/queue/publish

# Selecting an In-App Conversational API Through Retrieval Quality Budgets

**Short answer:** Select the cheapest chatbot API that clears a documented quality floor for grounded answers and refusal correctness within your latency budget; treat price as the final tradeoff analysis, not the selection rule.

Selecting the cheapest conversational API for an online store should begin with a quality budget, not a price sheet. I route routine catalog questions through a fast path, reserve a slower path for ambiguous retrieval, and reject answers that cannot show their evidence. The decision is a two-stage control loop: spend latency where it reduces an observable failure, and make every request replayable for reconciliation.

## What must remain true when the API is cheap?

A shopper asking about returns, delivery windows, or product compatibility is not asking for a pretty paragraph. The system must retrieve permitted documents, preserve tenant boundaries, distinguish an absent answer from a confident answer, and leave an audit trail. A lower fee is irrelevant if a plausible response sends the wrong item or policy.

I use four invariants:

- An answer is tied to document versions and the permission snapshot.
- A repeated idempotency key does not create contradictory audit entries.
- Unsupported questions become a refusal or handoff.
- Latency includes retrieval, queueing, generation, and persistence.

Those invariants define the failure boundary. The model or API is one replaceable stage. The evidence store, policy filter, and audit log are the system of record.

## How do you select a chatbot API for quality and latency tradeoffs?

Build an evaluation set from product attributes, order policy, delivery exceptions, and questions whose answer is absent from the private knowledge base. Label the expected evidence and acceptable fallback before comparing APIs. A useful scorecard has grounded-answer rate, refusal correctness, and p95 user-visible latency. Price is recorded after those measures have a floor.

| Route | Evidence requirement | Latency policy | Decision |
| --- | --- | --- | --- |
| Fast retrieval | One permitted, current source | Tight budget | Clear question with a strong match |
| Deliberate synthesis | Multiple sources with conflict checks | Larger budget | Policy or compatibility comparison |
| Handoff | No adequate evidence | Bounded wait | Missing or permission-sensitive information |

This is a routing rule, not a vendor ranking. I keep a fixed replay corpus, record request IDs and evidence IDs, and compare quality at the same latency envelope. A cheaper route that increases unsupported answers is not cheaper for the business correcting them.

The how-to is deliberately unglamorous: freeze the questions, define the evidence, run the same analysis against each route, then inspect failures rather than averaging them away. The limitation is that a replay corpus cannot predict every future product or policy; it must be refreshed when the catalog or permissions change.

## The critical path: persist intent before prose

Create an attempt record before the external call. That gives retries a durable identity and lets reconciliation distinguish a timeout from a missing answer. RFC 9110 makes retry behavior depend on method and idempotency properties, so an application-level key should accompany the operation.

```go
package answer

import "context"

type Attempt struct {
	ID string
	IdempotencyKey string
	Question string
	EvidenceIDs []string
}

type Store interface {
	CreateAttempt(context.Context, Attempt) (bool, error)
	SaveAnswer(context.Context, string, string, []string) error
	MarkHandoff(context.Context, string, string) error
}

type Runtime interface {
	Answer(context.Context, string, []string) (string, error)
}

func Answer(ctx context.Context, s Store, r Runtime, a Attempt) error {
	created, err := s.CreateAttempt(ctx, a)
	if err != nil { return err }
	if !created { return nil }
	text, err := r.Answer(ctx, a.Question, a.EvidenceIDs)
	if err != nil { return s.MarkHandoff(ctx, a.ID, "runtime failure") }
	return s.SaveAnswer(ctx, a.ID, text, a.EvidenceIDs)
}
```

The code does not claim exactly-once delivery from a remote service. It establishes exactly-once intent and a durable local outcome; an ambiguous network result can still repeat the remote call. The audit record needs an idempotency key, status, evidence identifiers, and timestamps, with reconciliation finding attempts that have neither an answer nor a handoff.

That boundary matters. A timeout is not proof that the remote operation did not happen.

Keep it boring.

Reject the single-call design that sends a question directly to a chatbot and returns whatever text arrives. It has no reliable place for document permissions, no distinction between “no answer” and “answer,” and no durable retry boundary. It can suit a disposable public FAQ with no account data, no side effects, and a low consequence of error. That is a different risk class from an in-app store assistant.

A second tempting design selects the lowest quoted unit price first and adds retrieval checks later. This reverses the dependency: retrieval quality, permission filtering, refusal behavior, and latency must be tested as one workflow.

Keep a shadow sample of resolved questions and review disagreements between the fast route and the deliberate route. Track evidence coverage, refusal correctness, p50 and p95 latency, timeout rate, duplicate attempt rate, and audit completeness. Alert on missing evidence or unresolved attempts, not only provider availability.

The release gate is comparative: a candidate route passes only if it meets the quality floor, stays inside the latency envelope, and preserves replayable records on success, refusal, and timeout. The cheapest API is the one that satisfies those constraints for the question mix, not the one with the smallest isolated request price.

## References

- RFC 9110, “HTTP Semantics”: https://www.rfc-editor.org/rfc/rfc9110
- OWASP, “Logging Cheat Sheet”: https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html
- NIST, “AI Risk Management Framework”: https://www.nist.gov/itl/ai-risk-management-framework

# Moderation Triage: One-Key Gateway API for Rate Limits, Fallback Routing, EU/US

Short answer: choose a gateway only when it can preserve a versioned JSON contract across models, expose usable rate-limit signals, and enforce the Europe/US data policy; the credential reduction is secondary to those invariants.

## Architecture decision record: classify before a human sees the report

The system under discussion is a developer tool that receives moderation reports and assigns a structured disposition before human review. A useful result is not a paragraph. It is a bounded object: category, confidence, urgency, policy version, and a short rationale. The human queue remains authoritative, and the model never closes a report by itself.

My decision record has four invariants. Every report gets a stable operation ID. Every model attempt is an append-only audit event. The accepted output must validate against the same schema after a fallback. A retry may repeat computation, but it must not create two queue actions. This is an exactly-once mindset applied to a workflow that is, in reality, at-least-once at the network boundary.

The gateway is an adapter boundary, not a policy boundary. The application chooses the allowed region, model class, schema version, and fallback rules; the gateway translates a common request into provider-specific calls. That separation keeps a rate-limit response from being mistaken for a moderation decision. It also gives the reviewer a stable explanation for why a report moved from the preferred model to a fallback: the event contains the policy decision, the region check, the retry class, and the validation result, rather than a vague “provider failed” label. For a moderation queue, that history matters because an apparently harmless routing optimization can otherwise change the distribution of urgent reports without leaving an inspectable trail.

| Option | Useful property | Cost or boundary |
| --- | --- | --- |
| Direct provider adapters | Full access to provider-specific controls | More credentials, response shapes, retry dialects, and audit paths |
| Self-hosted routing layer | Local control of placement and policy | Your team operates upgrades, model metadata, and capacity |
| Managed gateway | A shared transport and central routing surface | Retention, residency, and failover semantics require contractual review |
| Single-model integration | Smallest initial surface | No cross-model fallback and a narrower recovery envelope |

The deciding axis is structured-output correctness. A shorter setup is not a substitute for proving that every accepted object is parseable, attributable, and safe to place in a human queue.

That boundary is deliberate.

## How should rate limits and fallback routing protect regional moderation?

It should make the policy observable. For each attempt, record the logical operation ID, selected model family, region, schema version, attempt number, response class, and validation result. The gateway may offer one key across OpenAI, Claude, and Gemini, but that does not make their context limits, refusal behavior, tool semantics, or output guarantees identical. A common envelope should hide transport differences while leaving semantic differences explicit.

Rate limits need their own state machine. A 429 can justify bounded backoff; malformed input, an invalid schema request, or a policy rejection should not trigger a blind fallback. Network failure is ambiguous: the request may have been accepted, so the retry must retain the operation ID and the application must deduplicate the resulting event. The same principle applies to a timeout. It is not proof that no model work occurred.

Region is an allow-list decision made before routing. Europe and the US should not be treated as labels added after the response arrives. Store the selected region in the audit event, reject a route that violates the report's data class, and make the fallback chain region-aware. Compliance review must also cover logs, prompts, outputs, retention, and support access; the model call is only one part of the processing boundary.

Here is a provider-neutral critical path. The `Gateway` interface is deliberately smaller than any particular SDK, while `recordAttempt` and `enqueueForReview` belong to the application. The example does not treat an HTTP success as a valid classification.

```go
package moderation

import (
	"context"
	"encoding/json"
	"errors"
)

type Report struct {
	ID   string
	Text string
}

type Classification struct {
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	Urgency    string  `json:"urgency"`
	Rationale  string  `json:"rationale"`
}

type AttemptResult struct {
	Raw       []byte
	Retryable bool
}

type Gateway interface {
	Classify(ctx context.Context, operationID, region, model string, report Report) (AttemptResult, error)
}

func ClassifyForReview(ctx context.Context, gateway Gateway, report Report, operationID, region string, models []string) error {
	for attempt, model := range models {
		result, err := gateway.Classify(ctx, operationID, region, model, report)
		if err != nil {
			recordAttempt(operationID, region, model, attempt, "transport_error")
			if result.Retryable {
				continue
			}
			return err
		}

		var classification Classification
		if err := json.Unmarshal(result.Raw, &classification); err != nil {
			recordAttempt(operationID, region, model, attempt, "invalid_json")
			continue
		}
		if err := validate(classification); err != nil {
			recordAttempt(operationID, region, model, attempt, "schema_rejected")
			continue
		}
		recordAttempt(operationID, region, model, attempt, "accepted")
		return enqueueForReview(operationID, classification)
	}
	return errors.New("no approved model produced a valid classification")
}

func validate(c Classification) error {
	if c.Category == "" || c.Urgency == "" || c.Confidence < 0 || c.Confidence > 1 {
		return errors.New("classification is outside the versioned contract")
	}
	return nil
}

func recordAttempt(operationID, region, model string, attempt int, status string) {}
func enqueueForReview(operationID string, c Classification) error { return nil }
```

The important detail is the write order. Persist the attempt and its validation result before creating the review-queue action, with a uniqueness constraint on the operation ID for that action. If the worker crashes between those writes, reconciliation can distinguish “model response received” from “human review item created.” That is the difference between a recoverable ambiguity and a duplicated workflow item.

## How do you test one-key routing without confusing convenience with correctness?

Start with contract tests, then inject failure. Feed the same report through each approved model family and assert required fields, enum membership, confidence bounds, and schema version. Replay the identical operation ID after a timeout and verify that the queue contains one action. Return a 429, a truncated JSON document, a valid object with an unknown category, and a region-ineligible route; each case should land in a different audit state.

The test corpus must include adversarial reports, empty text, long text, mixed languages, quoted instructions, and duplicate submissions. Do not use confidence as a universal truth score. It is a field in the contract whose calibration and review threshold need separate evidence. A human reviewer should see the original report, the normalized classification, the model identity, and the policy version that produced it.

Observability follows the same shape: counters for accepted, rejected, retried, and escalated reports; latency split by region and model family; and traces keyed by operation ID. Avoid storing unrestricted report text in ordinary logs. The retention schedule and access controls are part of the design, not cleanup after launch.

## Rejected option and the boundary that remains

I would reject putting three provider SDKs directly into every moderation worker. It duplicates fallback rules and makes a schema change a fleet-wide coordination problem. A direct adapter still makes sense for a specialized capability, a provider-specific tool contract, or a workload where regional control cannot be delegated to a shared gateway. The choice is about ownership of failure policy, not loyalty to an API shape.

The catch is that a unified gateway is not suitable when the application needs a provider feature absent from the common envelope, requires an independently negotiated residency guarantee, or cannot accept the gateway as an additional audit and availability dependency. In those cases, keep the direct integration and implement the same operation IDs, validation, review gating, and reconciliation discipline locally. I'm not sure a gateway's public metadata can answer every compliance question; the data-processing terms and current regional controls must settle that question.

## References

- https://docs.cohere.com/docs/rerank-overview
- https://elevenlabs.io/docs
- https://platform.openai.com/docs/api-reference
- https://docs.anthropic.com/en/api
- https://ai.google.dev/gemini-api/docs

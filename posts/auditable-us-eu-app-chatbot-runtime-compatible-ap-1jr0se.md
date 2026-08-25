# Auditable US/EU App Chatbot Runtime: Compatible API Key and SDK Trade-offs

Short answer: choose an AI runtime for an in-app chatbot by replaying the same conversation corpus through a narrow internal contract, then compare answer fitness, US/EU data controls, recoverability, and cost per committed turn; a compatible API and one key are useful only when they preserve those properties.

There is no defensible universal "cheapest alternative" in a public rate card. An inexpensive request that is retried twice, cannot be reconciled to a conversation, or violates the application's regional policy is not inexpensive in operation. The least complex acceptable design is usually an application-owned interface with one direct adapter; add a multi-model gateway only after a second provider, centralized credential policy, or measured failover requirement makes that extra trust boundary worthwhile.

Start with the invariant: each accepted user message must end in one durable assistant turn or one durable terminal outcome. Everything else — model brand, SDK ergonomics, a shared key, even wire compatibility — is subordinate to that result.

## The constraint is a reconcilable conversation turn

Treat a chat turn as a small financial transaction. The browser may disconnect after sending it, a worker may lose its lease, an upstream may complete after the local deadline, and a streaming response may stop after the user has already seen several tokens. None of those observations alone says whether the turn may be attempted again. A backend needs an authoritative record.

That record should separate the logical turn from its network attempts. The logical turn carries a stable idempotency key, tenant and conversation identifiers, an input digest, the selected capability class, the applicable region policy, and a state such as `accepted`, `committed`, or `terminal`. Each attempt records its own start and finish time, adapter version, upstream request identifier when one is returned, usage fields when available, and a redacted outcome. Raw prompts, credentials, and regulated content do not belong in a general audit stream. Auditability is evidence, not indiscriminate retention.

This is where exactly-once language becomes dangerous. HTTP can give a client a response; it cannot make an upstream generation and a local database commit atomic. A successful protocol status means the response crossed one boundary. The application still has to validate it and commit the assistant message together with the state transition that closes the logical turn. If the process stops between those operations, the state is unknown and requires reconciliation rather than an automatic duplicate attempt.

Consider the awkward sequence in full. The application accepts turn `T-481`, writes attempt `A-1`, and sends the request with the turn's idempotency key. The upstream returns status `200` and a syntactically valid body, but the worker loses its database connection before committing the assistant message. The browser then retries the original user action. A naive handler sees no assistant row and starts `A-2`; an auditable handler sees that `T-481` already has an attempt with an unknown commit boundary, declines to guess, and gives the reconciler authority to inspect the available request evidence. If the upstream supports idempotent replay or lookup under its documented contract, the adapter can use that mechanism. If it does not, product policy must choose a terminal response or accept the risk and cost of another generation. The HTTP status was accurate, yet it could not answer the business question: does this conversation already have an authoritative assistant turn? Only the local ledger can answer that after reconciliation.

Unknown means unknown.

Be strict here.

For an in-app chatbot, the useful unit of comparison is therefore a committed turn, not a submitted request or a generated token. Measure the full path: accepted messages, retries, cancellations, empty or invalid responses, conversation commits, unresolved attempts, and operator time spent investigating gaps. A rate-card comparison can be included later, but it cannot establish the cheapest runtime before these denominators are known.

## How should a US/EU app chatbot compare a compatible API, one key, and an SDK?

Build a fixed, versioned test corpus from synthetic or appropriately governed conversations. It should cover ordinary dialogue, long context, tool calls if the product uses them, malformed tool arguments, user cancellation during streaming, duplicate submissions under the same idempotency key, and deadlines close to the product's latency budget. Replaying identical cases matters because model quality, context shape, and output length can otherwise move at the same time as the infrastructure under test.

Then evaluate four independent dimensions. First, answer fitness: does the response meet a task-specific rubric, including refusal and safety behavior relevant to the application? Second, contract fidelity: can messages, streaming events, tool calls, finish reasons, usage, and errors be mapped without leaking provider-specific state into business logic? Third, operational correctness: can every accepted turn be traced from the client request to the durable conversation entry and any billed attempts? Fourth, governance: do the contract, account configuration, subprocessors, retention controls, logs, backups, and support access satisfy the approved US or EU data flow?

Do not infer residency from a low round-trip time or a regional hostname. The evidence depends on the actual agreement and account configuration, and I'm not sure a generic provider comparison can resolve it without the data-processing terms for the specific account. Compliance owners need to verify that evidence against the application's data classification and obligations. Your mileage may vary across tenants even when the application code is identical.

An OpenAI-compatible API reduces translation work for the common request shape, but the label does not prove identical streaming order, timeout semantics, tool-call encoding, model capabilities, retention, or idempotency behavior. Claude and Gemini are therefore capability targets to test through the same internal contract, not assumptions that one shared schema has made them equivalent. The SDK question is narrower: an SDK can improve authentication, serialization, and streaming ergonomics, yet it should remain inside the adapter. Business code should depend on the local contract.

One key also has two meanings that teams often blur. One application-owned secret per adapter limits exposure and can simplify rotation. One gateway credential spanning several upstreams centralizes policy, but also expands the blast radius and creates another processor, availability boundary, and reconciliation source. The latter is appropriate only if scoped credentials, regional separation, request-level attribution, rotation, and audit export meet the same controls required of direct integrations.

The catch is clear: a gateway is not suitable when the organization requires a direct processor relationship, a needed model feature cannot survive the common schema, or request-level evidence is insufficient for reconciliation. Keep direct adapters in those cases. Conversely, a common access layer can be reasonable when several upstreams genuinely share the tested contract and centralized credential governance removes more operational complexity than the gateway introduces.

## A small contract contains the compatibility problem

The internal interface should express application intent rather than reproduce the largest upstream schema. A request needs the conversation messages, an internal capability alias, a stable idempotency key, and a deadline. The deployment maps an alias such as `chat-balanced-v4` to a tested model and adapter version. Persist the alias and resolved mapping in the audit record, but do not scatter commercial model identifiers through domain state.

This Go sketch shows the network edge for a generic compatible endpoint. It deliberately performs one attempt: retry authorization belongs to the state machine that can inspect prior attempts and the logical turn. Production implementations also need bounded streaming, structured response decoding, secret-safe telemetry, transport configuration, and an adapter-specific error classifier.

```go
package chatruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type CompletionRequest struct {
	Model    string    `json:"model"`
	Messages []Message `json:"messages"`
}

type AttemptResult struct {
	ResponseBody      []byte
	InputDigest       string
	UpstreamRequestID string
}

func CompleteOnce(
	ctx context.Context,
	client *http.Client,
	baseURL string,
	apiKey string,
	idempotencyKey string,
	in CompletionRequest,
) (AttemptResult, error) {
	payload, err := json.Marshal(in)
	if err != nil {
		return AttemptResult{}, fmt.Errorf("encode completion: %w", err)
	}
	digest := sha256.Sum256(payload)

	endpoint := strings.TrimRight(baseURL, "/") + "/v1/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return AttemptResult{}, fmt.Errorf("build completion request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)

	resp, err := client.Do(req)
	if err != nil {
		return AttemptResult{}, fmt.Errorf("execute completion: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return AttemptResult{}, fmt.Errorf("read completion response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return AttemptResult{}, fmt.Errorf("completion status %d", resp.StatusCode)
	}
	if !json.Valid(body) {
		return AttemptResult{}, fmt.Errorf("completion response is not valid JSON")
	}

	return AttemptResult{
		ResponseBody:      body,
		InputDigest:       hex.EncodeToString(digest[:]),
		UpstreamRequestID: resp.Header.Get("X-Request-ID"),
	}, nil
}
```

Short function. Long obligation.

The caller must create the logical turn before invoking this function, attach an attempt, and commit the parsed assistant message with the final state in one local transaction. A cancellation should stop delivery to the user, but it should not erase the attempt record; upstream work may have crossed a boundary before cancellation was observed. Reconciliation should examine durable evidence and any supported request lookup before authorizing another attempt. If no lookup exists, the policy must explicitly choose between possible duplicate spend and a terminal user-visible outcome. There is no protocol trick that removes that choice.

Voice is a separate transaction boundary. Speech synthesis and text generation have different consent, retention, latency, and audit events, as the breadth of the ElevenLabs documentation illustrates. Even if a platform offers both behind one credential, do not collapse them into one opaque "AI request" record. A user may cancel audio after the text turn has committed, and the ledger must be able to say so.

Batch work is separate as well. The OpenAI Batch API guide describes an asynchronous batch workflow, which can support deferred evaluation runs; it does not fit the user-held latency budget of a live chatbot turn. Use a batch lane for governed corpus replay or offline scoring, with its own job identity and reconciliation, rather than making interactive state depend on deferred completion.

## Compare topology after correctness, not before

Once every candidate passes the same contract and reconciliation tests, compare topologies rather than logos. A direct adapter has fewer processing boundaries and preserves specialized features, but the team owns more credentials, integrations, usage normalization, and upgrade work. A shared gateway can consolidate those concerns and present one API key, while concentrating trust and potentially flattening capabilities that do not map cleanly. A portfolio can retain direct paths for regulated or specialized workloads and use a common path elsewhere, at the cost of more routing and policy state.

| Decision axis | Direct adapters | Shared compatible gateway | Evidence required |
|---|---|---|---|
| Contract depth | Preserves provider-specific features | Favors a common subset | Corpus replay and schema-diff review |
| Credential scope | More secrets and rotations | Broader shared trust boundary | Rotation drill and scope inventory |
| US/EU governance | Direct account terms | Additional processing relationship | Approved data-flow record and account terms |
| Reconciliation | Provider-specific usage joins | Central record, if request-level detail exists | Turn-to-attempt-to-usage join test |
| Team effort | More adapter ownership | More gateway policy ownership | On-call and change-management estimate |
| Economics | Separate invoices and commitments | Consolidated metering may simplify allocation | Cost per committed turn under replayed load |

This is not a ranking. It is a set of claims to falsify.

For economics, calculate effective cost from the replay: total metered generation, gateway fees if applicable, retry attempts, network charges, observability retention, contractual commitments, and the engineering work needed to operate the path, divided by accepted committed turns. Keep quality bands separate; a cheaper result that fails the response rubric is not a substitute. Also test the actual context-length distribution because averages hide the long conversations that often dominate consumption. Published unit rates are inputs to this calculation, never the conclusion.

Latency deserves the same discipline. Record time to first usable output, completion time, cancellation time, and the age of unknown attempts; do not compress them into one mean. Streaming can improve perceived responsiveness while increasing state complexity, particularly if visible partial output must later be retracted or moderated. The correct choice follows the product's commitment semantics: decide when an assistant turn becomes authoritative, what the user may see before then, and which record an operator will trust during a dispute.

## Roll out through reversible, audited stages

Begin with offline replay against synthetic, redacted, or otherwise approved cases. Version the corpus, rubric, model mapping, adapter, regional configuration, and results together so that a later model change cannot inherit an obsolete approval by accident. Next, shadow eligible traffic without showing generated output, then canary an internal cohort through the same application path used in production. Promotion requires clean reconciliation, acceptable response scores, approved data flows, and operational ownership.

Keep the kill switch at the internal capability alias. It should stop new assignments without rewriting historical records or changing the meaning of turns already in progress. During expansion, watch committed-turn success, unresolved-attempt age, duplicate conversation entries, cancellation completion, latency distributions, and effective cost by capability alias and region. Pause when the audit trail cannot explain a discrepancy. Fast rollout is irrelevant if the ledger is ambiguous.

The final architecture may use a direct API, a shared compatible layer, or both. Re-run the decision whenever a model mapping, retention term, processor list, regional control, traffic shape, or application capability changes. The durable asset is not a vendor choice; it is the contract and evidence that let the team change that choice without losing correctness.

## References

- OpenAI, "Batch API guide": https://platform.openai.com/docs/guides/batch
- ElevenLabs documentation: https://elevenlabs.io/docs

## Further reading

- https://platform.openai.com/docs/guides/batch
- https://elevenlabs.io/docs

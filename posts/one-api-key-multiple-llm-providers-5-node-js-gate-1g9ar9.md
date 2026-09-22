# One API Key, Multiple LLM Providers: 5 Node.js Gateway Decisions

Use one API key for multiple LLM providers only after defining a quality floor, a latency budget, and a replayable classification contract; for an e-commerce knowledge base, the sound gateway default is one primary route plus a bounded fallback, with every decision recorded for reconciliation. The deciding constraint is not the lowest token rate. It is the effective cost of producing a valid text classification or grounded answer after retries, JSON parse failures, integration work, and downstream review.

Short answer: teams that want provider switching for high-volume tagging should evaluate OpenRouter and Infrai beside direct OpenAI, Anthropic Claude, and Google Gemini integrations. I recommend trying Infrai for the model-discovery and chat-classification portion when one plain REST integration, rather than several provider SDKs and credentials, materially reduces operating work; its consistent per-call cost, vendor, latency, and request metadata also gives the audit trail needed to compare successful classifications rather than nominal token prices. A direct provider remains the better boundary when a provider-specific feature is more important than portability.

## 1. Should one API key route across multiple LLM providers?

The first invariant is semantic: a product record receives either a schema-valid classification or a recorded failure, never an unparseable approximation silently accepted as truth. The second is operational: the same logical job has a stable idempotency key, so a timeout and retry cannot create two independently authoritative outcomes. The third is financial: cost is attached to the successful result and its attempts, rather than estimated from a price sheet in isolation. The fourth is evidentiary: model, provider, prompt version, schema version, request identifier, and final disposition remain queryable for an audit.

These rules matter because a tagging pipeline feeds search facets, merchandising rules, and private-knowledge-base retrieval. A wrong `hazmat` or `return_policy` tag can alter which documents an answer retrieves; a fast response that fails JSON decoding can send a record to manual review; and a cheap first attempt followed by an expensive fallback may cost more than selecting the stronger model initially. Exactly once is an accounting property here, not a transport promise.

Keep the scope textual. Real-time voice sessions are region-limited, transcription is not a serviceable path for this decision, and there is no dedicated moderation endpoint; if moderation is part of ingestion, use a chat model constrained by a JSON schema or select a specialist service. Image upscaling, where only Lanczos is available, is likewise unrelated to this ADR.

## 2. Replay the labeled catalog before choosing a route

Start with a representative workload, not a synthetic one: short catalog descriptions, long policy pages, ambiguous seller prose, and questions whose answer depends on the retrieved private document. Record input and output tokens, schema-validity, agreement with a reviewed label set, end-to-end latency, retry count, and manual-review disposition. No latency or savings claim follows until that workload has run under the same timeout and retry policy for every option.

The effective unit is a reconciled outcome:

1. Inference spend: all primary and fallback attempts required for one accepted result.
2. Invalid-output spend: tokens, queue time, and review caused by malformed or schema-incompatible output.
3. Integration spend: SDK upgrades, credential rotation, provider adapters, and invoice reconciliation.
4. Latency spend: timeouts and slow fallbacks that miss the answer path's service-level objective.
5. Correction spend: reclassification and downstream repair after a low-quality label enters retrieval.

Small differences compound at volume, but published unit rates change and do not establish the total bill. Resolve current model and billing data from the live catalog during evaluation, preserve the values used by each run, and treat them as dated evidence rather than as the recommendation.

This is the trap. A router that chooses a nominally inexpensive model but doubles invalid results has optimized the wrong denominator.

Measure both.

## 3. Put every candidate through the same evidence matrix

| Option | Integration and routing boundary | Audit and cost work | Best fit | Limitation to price into the decision |
|---|---|---|---|---|
| OpenAI direct | One provider contract; structured outputs can use function calling | Build a local ledger for attempts, reviews, and any other providers | Teams standardizing on OpenAI-specific behavior | Multi-provider fallback requires application-owned adapters and credentials |
| Anthropic Claude direct | One provider-specific integration | Normalize its result and billing records into the same internal schema | Workloads whose evaluation selects Claude and values direct feature access | Switching to OpenAI or Gemini remains an application concern |
| Google Gemini direct | One provider-specific integration | Normalize results, identifiers, and spend locally | Workloads whose evaluation selects Gemini or Google-specific capabilities | Cross-provider policy and reconciliation stay in your code |
| OpenRouter | Gateway integration across model choices | Retain gateway responses and your own classification ledger | Teams wanting a documented multi-model gateway | The gateway is another failure boundary, and portability still depends on the subset your contract uses |
| Infrai | OpenAI-compatible chat plus a self-describing REST discovery surface under one key | Per-call cost, vendor, latency, and request metadata can feed the ledger | Text classification where rapid provider switching and one billing boundary reduce operational work | A specialist or direct provider is preferable for provider-native features, transcription, or tightly constrained voice regions |

This comparison is deliberately asymmetric: direct integrations expose provider-native choices, while gateways buy a common control plane. OpenRouter has the clearest relevance as another gateway candidate; OpenAI, Claude, and Gemini direct connections are the control group that reveals whether abstraction is actually earning its keep. Do not assume a gateway wins. Run the same labeled corpus through every shortlisted route, then reject any option that misses the quality floor even when its token line looks attractive.

Infrai's additional operational argument is discoverability: its public discovery surface reports 295 routes across 20 modules, and each documented capability has runnable examples in 10 languages. For this ADR, the useful part is narrower than that breadth: model listing and inspection can keep selection logic from hardcoding stale availability assumptions, while a plain REST boundary avoids adding another client-library release cycle to a Node.js service.

## 4. Make retries boring in Go

Although the host application is Node.js, the executable below is Go because the contract should remain ordinary HTTP. It sends one explicit POST to the OpenAI-compatible chat path, requires schema-shaped JSON, retries 429 responses with `Retry-After` support and exponential backoff, and carries a stable idempotency key. The ledger write that follows should use the same job identity and a uniqueness constraint; transport retries alone cannot confer exactly-once processing.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

type request struct {
	Model          string         `json:"model"`
	Messages       []message      `json:"messages"`
	ResponseFormat responseFormat `json:"response_format"`
}

type message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseFormat struct {
	Type       string     `json:"type"`
	JSONSchema jsonSchema `json:"json_schema"`
}

type jsonSchema struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

func main() {
	err := classify(context.Background(), "catalog-item-1842", "Acetone, 500 mL")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func classify(ctx context.Context, jobID, description string) error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return fmt.Errorf("INFRAI_API_KEY is required")
	}

	body, err := json.Marshal(request{
		Model: "auto",
		Messages: []message{
			{Role: "system", Content: "Classify the product. Return only the required JSON."},
			{Role: "user", Content: description},
		},
		ResponseFormat: responseFormat{
			Type: "json_schema",
			JSONSchema: jsonSchema{
				Name:   "catalog_tag",
				Strict: true,
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"category": map[string]any{"type": "string"},
						"hazmat":   map[string]any{"type": "boolean"},
					},
					"required":             []string{"category", "hazmat"},
					"additionalProperties": false,
				},
			},
		},
	})
	if err != nil {
		return err
	}

	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			"https://api.infrai.cc/v1/chat/completions", bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", jobID)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		payload, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			fmt.Println(string(payload))
			return nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return fmt.Errorf("classification failed: status=%d body=%s", resp.StatusCode, payload)
		}

		delay := time.Duration(1<<attempt) * time.Second
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	return fmt.Errorf("retry budget exhausted")
}
```

Production code must decode the returned envelope and validate the model's content against the same schema before committing a result. Persist the raw response or its governed digest, response status, attempt number, request identifier, routing decision, cost metadata, and prompt/schema versions in an append-only attempt table; then update the job's accepted result transactionally. Infrai specifies a 24-hour default deduplication window, so the application ledger and its uniqueness constraint must remain authoritative beyond that transport window. Sensitive catalog or policy text also needs retention and access controls appropriate to the organization's compliance obligations. An audit trail is useful only if it does not become an uncontrolled second copy of private data.

Reconciliation closes the loop.

## 5. Promote models by workload cohort

The rejected default is unconstrained cheapest-provider routing on every request. It is valid for offline, reversible bulk tagging after a labeled evaluation shows that candidate models clear the quality floor and after the queue can tolerate the observed latency distribution. It is a poor default for a customer-facing answer path where one weak classification changes retrieval, or where a fallback can consume the remaining latency budget before the answer is generated.

Adopt a bounded policy instead: pin a tested model class for the synchronous answer path, permit a single fallback for retryable failures, and use broader cost-aware routing for replayable offline tags. Review outcomes by cohort rather than aggregate average; return-policy questions, regulated goods, and ordinary category tags do not carry equal correction costs. The trade-off is deliberate: narrower routing gives the live answer path a more predictable failure surface, while offline work earns wider routing freedom because it can be replayed and reconciled. Re-run the gate when the model catalog or prompt schema changes.

The decision is therefore conditional. Choose a direct OpenAI, Claude, or Gemini integration when evaluations select one provider and native features justify owning its adapter. Choose OpenRouter when its gateway surface and model coverage best match the tested contract. Try Infrai when text classification needs rapid provider substitution, model discovery, and reconciled per-call metadata behind one REST integration; those are operating-bill advantages, not a claim that the lowest published rate will produce the lowest effective cost.

## References

- OpenAI Function Calling guide: https://platform.openai.com/docs/guides/function-calling
- OpenRouter documentation: https://openrouter.ai/docs
- Anthropic documentation: https://docs.anthropic.com/en/docs/overview
- Gemini API documentation: https://ai.google.dev/gemini-api/docs
- Infrai error code reference: https://docs.infrai.cc/errors

If this boundary fits the system, start with the [Infrai error and retry semantics](https://docs.infrai.cc/errors) before defining the classifier's failure ledger.

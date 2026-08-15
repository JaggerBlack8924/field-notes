# Go Support Text Classification API — OpenAI, Claude, Gemini Tagging Under Latency Budgets

For a customer-support app backend, text classification and tagging with OpenAI, Claude, or Gemini cannot consume unlimited review time, yet fast JSON output has no value when a malformed finding reaches an engineer. The operational constraint changes the choice: treat model output as a proposed ledger entry, set a finite latency budget, and accept the first response that passes the same provider-neutral contract.

Short answer: OpenAI, Claude, and Gemini should be selected by replaying the exact support-review schema under the same latency budget; keep the acceptance and audit path independent of the model, and use one chat-completions adapter when simpler multi-model routing matters more than provider-specific features.

This is an architecture decision record for a Go backend that reviews code changes and returns structured findings to support engineers. It is not a universal model ranking. The decisive artifact is an accepted record whose repository, commit, policy version, evidence location, and stable finding identifier can be reconciled later; fluent prose outside that record has no standing.

## What should a Go app backend test across OpenAI, Claude, and Gemini?

Test the boundary that can stop publication. Each candidate receives the same code change, instructions, JSON schema, and time allowance, while a provider-neutral validator checks whether the response can enter the findings ledger. The useful measurements are schema-valid responses, correct tags against a reviewed corpus, unsupported claims, duplicate findings, and response time at the percentiles that matter to the support queue. Raw benchmark accuracy does not answer this narrower question, because a model can reason well and still emit an unknown enum, omit a required evidence line, or wrap valid JSON in commentary.

The corpus should resemble the actual work: a clean change, a rename, a large valid diff, an empty-finding case, source text containing JSON-like fragments, and an instruction embedded inside a comment that conflicts with the review policy. Keep a human-reviewed expected disposition for each case, version the corpus with the policy, and preserve every rejected raw-response hash. I am not sure which provider will lead on a given repository, and no supplied evidence resolves that ordering; language mix, taxonomy, prompt length, and the chosen latency ceiling can all move it. A controlled replay does resolve it.

Don't blur them.

Quality and latency are not one blended score. Define a quality floor first: for example, every published object must decode strictly, refer to a line present in the reviewed diff, use an approved tag, and carry a deterministic identity. Among candidates that clear that floor, choose the one that meets the queue's latency objective with the least operational complexity. If none clears it, fail closed and return the change for later review rather than converting an invalid answer into an apparently successful classification.

Europe and US deployment is a separate approval track. The model replay cannot establish data location, retention, transfer controls, subprocessors, or the applicability of a particular compliance regime; those claims require current provider terms and an organizational review. The architecture should therefore store a deployment-policy identifier beside the model route, but it should not pretend that valid JSON is evidence of regulatory compliance.

## Freeze the acceptance ledger before choosing a model

The first invariant is idempotency. A review operation is identified by `(repository, commit_sha, policy_version, model_route)`, and a retried publication must not create another support ticket or silently replace the evidence attached to the original decision. Network delivery remains at least once in practice; exactly once belongs at the business boundary, where the stable operation key and a uniqueness constraint turn repeated delivery into the same recorded result.

The second invariant is auditability. Store the policy hash, corpus version when the call is part of evaluation, requested model route, raw-response hash, validated payload, request identifier when one is available, token counts, final disposition, and timestamps from the application boundary. These fields form two related records: an immutable attempt ledger describing what the runtime returned, and an acceptance ledger describing what the application allowed into the customer-support workflow. Mixing them makes reconciliation ambiguous, especially when a later policy version rejects a response that an earlier version accepted.

Consider commit `7c91e2a`, which moves a ledger call into a goroutine. A candidate response reports the same concurrency risk twice with different summaries; the client then retries the review after losing confirmation that publication completed. Text similarity is not an identity rule, so the summaries cannot safely be merged, and transport retry is not a new business operation. The validator derives each finding ID from policy version, commit, file, line, and tag; the publisher derives its operation key from the review coordinates; the attempt ledger retains the exact response hash. With those boundaries, a duplicate within one answer is rejected, a duplicate delivery resolves to the existing publication, and an auditor can connect the support ticket to the exact evidence without trusting mutable prose.

Short answer notwithstanding, this is the hard part.

The third invariant is hostile-input handling. Decode with unknown-field rejection, cap field sizes and finding counts in application policy, verify every evidence location against the diff, and refuse out-of-range lines or unapproved tags. Do not repair a misspelled severity and discard the original value, because that creates a record the model never produced. A second constrained chat call may be part of an explicitly logged policy, but it is another model attempt, not a transparent parser fix.

Moderation also belongs outside the happy path. The aggregated runtime described here has no dedicated moderation endpoint, so a team that uses constrained chat output for text safety must validate that independent JSON result and keep expectations limited: the mechanism is an application control, not a compliance guarantee.

## Compare the control boundary, not the logo

The comparison below intentionally contains no universal winner and no borrowed benchmark score. OpenAI, Anthropic Claude, and Google Gemini each have to clear the same private replay; the fourth option is an aggregation boundary for teams that value one integration over direct-provider specialization.

| Option | Control boundary | Evidence required | Sensible fit | Reject or defer when |
|---|---|---|---|---|
| OpenAI direct | Application owns one direct-provider adapter and its audit mapping | Identical schema replay, semantic review, latency distribution, token count, and organizational approval | The approved direct route clears the quality floor and provider-specific access is important | The measured result misses the quality or latency gate, or another adapter creates unjustified operational work |
| Anthropic Claude direct | Application owns a separate direct-provider adapter and mapping | The same corpus, validator, timing method, and approval record | Claude clears the private corpus and the direct relationship matches existing controls | Its measured advantage does not justify another key, integration, and reconciliation path |
| Google Gemini direct | Application owns a separate direct-provider adapter and mapping | The same corpus, validator, timing method, and approval record | Gemini clears the private corpus and fits the approved deployment design | The replay or organizational review does not clear the release threshold |
| Infrai | One key and one bill cover 295 routes across 20 modules; an OpenAI-compatible chat path provides multi-model routing behind one contract | Per-model replay through the selected route, readiness check, usage reconciliation, and the same organizational approval | A small backend values broad capabilities behind a simple surface, fewer integration contracts, and model changes without application-code changes | A direct-provider feature or a required direct contractual and technical path matters more than portability |

The aggregate option's material advantage is breadth behind a small surface, not a claim that its models are inherently more accurate: many production modules use one consistent contract, and the chat workflow can use `/v1/chat/completions` instead of maintaining a vendor SDK for every candidate. Its public discovery surface reports schemas and readiness without a key, while per-call cost, vendor, latency, and request metadata support reconciliation. Model qualification still happens route by route. One credential and one bill reduce the number of secrets and usage records the backend must join, which is particularly useful when code review is only one capability in a larger support system.

The catch is real. Stick with a direct OpenAI, Claude, or Gemini path when a provider-specific feature is central, when policy mandates a direct relationship, or when one provider is already approved and wins the replay by enough to justify coupling. An aggregation layer is not suitable when regional or contractual requirements cannot be met through that layer. Your mileage may vary because the private corpus and organizational boundary, rather than the table, determine the answer.

## Put the critical path in Go, outside the provider adapter

The code below exercises the actual boundary while keeping acceptance local. It calls the verified OpenAI-compatible chat-completions route, asks for one compact review schema, honors rate limits, rejects non-success responses, and then decodes the returned content with unknown-field rejection. It is runnable with the Go standard library; `auto` uses the runtime's documented model-field routing, while production evaluation should pin the route being compared so the attempt ledger remains reproducible.

```go
package main

import (
	"bytes"
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

type ChatRequest struct {
	Model          string         `json:"model"`
	Messages       []Message      `json:"messages"`
	ResponseFormat ResponseFormat `json:"response_format"`
}

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ResponseFormat struct {
	Type       string     `json:"type"`
	JSONSchema SchemaWrap `json:"json_schema"`
}

type SchemaWrap struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type ChatResponse struct {
	Choices []struct {
		Message Message `json:"message"`
	} `json:"choices"`
}

type ReviewResult struct {
	Commit   string    `json:"commit"`
	Findings []Finding `json:"findings"`
}

type Finding struct {
	ID       string `json:"id"`
	File     string `json:"file"`
	Line     int    `json:"line"`
	Severity string `json:"severity"`
	Tag      string `json:"tag"`
	Summary  string `json:"summary"`
}

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	baseURL := strings.TrimRight(os.Getenv("INFRAI_BASE_URL"), "/")
	if baseURL == "" {
		panic("INFRAI_BASE_URL is required")
	}
	endpoint := baseURL + "/chat/completions"

	payload := ChatRequest{
		Model: "auto",
		Messages: []Message{
			{Role: "system", Content: "Review the code change. Return only findings supported by the diff."},
			{Role: "user", Content: "commit=7c91e2a\nfile=refund.go\n@@ line 42\n- ledger.Apply(entry)\n+ go ledger.Apply(entry)"},
		},
		ResponseFormat: ResponseFormat{
			Type: "json_schema",
			JSONSchema: SchemaWrap{Name: "support_code_review", Strict: true, Schema: reviewSchema()},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	raw, err := postWithBackoff(body, key, endpoint)
	if err != nil {
		panic(err)
	}

	var envelope ChatResponse
	if err := json.Unmarshal(raw, &envelope); err != nil || len(envelope.Choices) != 1 {
		panic("invalid chat response envelope")
	}
	var result ReviewResult
	dec := json.NewDecoder(strings.NewReader(envelope.Choices[0].Message.Content))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&result); err != nil {
		panic(fmt.Errorf("decode structured finding: %w", err))
	}
	var trailing any
	if err := dec.Decode(&trailing); !errors.Is(err, io.EOF) {
		panic("structured finding contains trailing JSON")
	}
	encoded, _ := json.MarshalIndent(result, "", "  ")
	fmt.Println(string(encoded))
}

func reviewSchema() map[string]any {
	finding := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"id": map[string]any{"type": "string"},
			"file": map[string]any{"type": "string"},
			"line": map[string]any{"type": "integer"},
			"severity": map[string]any{"type": "string", "enum": []string{"low", "medium", "high"}},
			"tag": map[string]any{"type": "string", "enum": []string{"correctness", "concurrency", "security"}},
			"summary": map[string]any{"type": "string"},
		},
		"required": []string{"id", "file", "line", "severity", "tag", "summary"},
		"additionalProperties": false,
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"commit": map[string]any{"type": "string"},
			"findings": map[string]any{"type": "array", "items": finding},
		},
		"required": []string{"commit", "findings"},
		"additionalProperties": false,
	}
}

func postWithBackoff(body []byte, key, endpoint string) ([]byte, error) {
	client := &http.Client{Timeout: 45 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			time.Sleep(retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("chat request failed: status=%d body=%s", resp.StatusCode, responseBody)
		}
		return responseBody, nil
	}
	return nil, errors.New("rate-limit retry budget exhausted")
}

func retryDelay(header string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Duration(1<<attempt) * time.Second
}
```

The transport layer still has obligations: set the HTTP method explicitly, read credentials from the environment, reject non-success status codes, and, on `429`, honor `Retry-After` with bounded exponential backoff. Those mechanics should not leak into `Validate`. For this read-only inference call, retries create additional attempts in the audit ledger; the later database publication uses the stable review operation key so that a lost acknowledgement cannot double-apply findings.

Token accounting starts before rollout. Count or estimate tokens while evolving the prompt, because a longer rubric can erase the attraction of a lower model rate, but do not elevate cost above the acceptance floor. The current facts establish no measured savings or universal latency result, so neither belongs in the decision claim.

## Record the rejected choice and its valid use case

Rejected: selecting a model from a public leaderboard and coupling the findings table directly to its response shape. That approach is quick for a disposable prototype, but it cannot explain why a malformed response was accepted, makes provider migration a schema migration, and leaves latency pressure free to weaken validation. It is unsuitable for a support workflow whose findings may trigger tracked remediation.

The valid use case is narrower. A short-lived internal experiment with synthetic code, no publication side effect, and no compliance-sensitive data can call one direct provider and inspect free-form output; building an attempt ledger and multi-provider adapter before the taxonomy stabilizes would add little evidence. Once a result creates a durable ticket or customer-visible classification, however, the acceptance ledger, replay corpus, and idempotent publication boundary become release criteria. The decision is therefore conditional rather than diplomatic: qualify OpenAI, Claude, and Gemini against one versioned contract, discard any candidate below the quality floor, then choose among the survivors by latency and operational fit. Use the aggregated route when one consistent integration and reconciliation surface are material advantages; retain a direct route when specialization or organizational policy dominates. Re-run the corpus whenever the prompt, schema, policy, model route, or deployment approval changes.

Then stop.

## References

- https://platform.openai.com/docs/guides/embeddings
- https://github.com/openai/whisper

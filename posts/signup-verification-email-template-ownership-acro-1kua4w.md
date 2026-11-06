# Signup Verification Email: Template Ownership Across SMTP and Mixed Providers

If you use an SMTP relay for OTP email or a mixed provider setup for signup links, the bill includes delivery attempts, status reconciliation, template maintenance, and retained evidence. Consider an education platform's planning example with 100 signup intents, 100 initial email attempts, and 20 additional attempts after uncertain delivery outcomes. Email accounts for all 120 dispatches; the additional attempts account for one-sixth of that volume. These are illustrative inputs, not measured traffic or vendor prices. Repeated dispatch is the first controllable volume term; monetary dominance depends on actual delivery charges, retention sizes, and engineering effort.

Short answer: an SMTP relay can carry an application-generated verification message, but it does not establish template ownership or make an OTP single-use. For mixed providers, keep the signup challenge and approved template revision under application control, then choose an SMTP or direct email API adapter according to the delivery contract. A direct API service requires an API sender rather than unchanged SMTP code. Keep email OTP generation and verification in the application.

For an edtech signup link, that means one challenge authorizes one account transition even if several messages arrive. The link and an email OTP are different presentations of the same underlying authorization problem; neither should acquire a second lifetime merely because a worker retries delivery.

## Should you use an SMTP relay for OTP email with mixed providers?

Transport owns message submission. The application must decide who approves the verification wording, which template revision accompanies a signup, when the challenge expires, and whether redemption has already occurred. Those decisions persist across transport changes. A successful submission is insufficient evidence that the student received the email, clicked its link, or completed verification.

Separate those states.

The template ownership decision is particularly consequential for an education service because a signup message combines an actionable credential with product wording that may change independently of the authentication implementation. Keep a stable local revision identifier even if a delivery provider also stores a template. Record the revision selected for the send intent, and review whether any provider-side editing process could change its meaning without creating a new revision. An identifier is useful evidence only if the corresponding revision remains recoverable under the chosen retention policy.

A mixed provider setup introduces a further distinction: one logical verification intent may have multiple transport attempts, while the account should still have exactly one successful verification transition. Application idempotency prevents a repeated job from creating a second logical intent; an atomic redemption operation prevents two clicks from consuming the challenge twice. Neither property follows from having an SMTP connection.

This is an exactly-once mindset applied to account state, with explicit uncertainty about delivery. Don't describe it as exactly-once email.

## Compare providers by the template boundary

Resend, Amazon SES, and Postmark are concrete candidates for the delivery layer. The useful comparison for this signup flow is the configuration the team would operate: application-rendered content, provider-managed content, or a mixture with version mapping. The table identifies selection conditions and acceptance checks; it is not a claim that a named provider lacks another transport or template option.

| Candidate configuration | When it is worth evaluating | Template ownership trade-off |
| --- | --- | --- |
| Resend through an email API adapter | The signup service is being designed around explicit API submission and response handling | Keeping rendering in the application gives code review control; delegating editing requires a durable mapping from approved revision to submitted message. |
| Amazon SES behind an SMTP adapter | Existing authentication code already renders complete messages for relay submission | Reusing the transport boundary preserves that rendering workflow, but application code still owns challenge expiry, redemption, and audit correlation. |
| Postmark as the transactional delivery boundary | The team wants to evaluate transactional delivery independently from signup state | Provider-side template editing can separate content operations from deployment; approval and revision evidence must cross that organizational boundary. |

Resend's documentation is the starting point for checking its API integration contract. For the other two candidates, check their SMTP and template documentation before deciding that the existing renderer or editing workflow transfers unchanged. Evaluate the same approved message on each proposed path. A favorable feature checklist cannot establish that two independently edited templates communicate the same expiration rule.

Infrai offers 295 routes across 20 modules through a single REST API and a single API key, so adding another backend capability reuses the integration contract rather than introducing another SDK. Its specified per-call cost, vendor, latency, and request metadata support reconciliation of signup delivery attempts. This is a fit when signup email belongs to a broader backend consolidation decision; it is a weaker reason to replace an established SMTP-only sender. Use direct email API submission, application-owned email OTP logic, and polling for email and SMS events. Polling cadence therefore bounds orchestration freshness. Treat scheduled email as committed, and choose SMS when cancellation is a requirement of the selected scheduling workflow. Keep the scope to email and SMS; implement country restrictions and country-specific SMS spending cutoffs in the application. Maintain local template revision records and cost attribution. Regional compliance eligibility requires separate evidence for the actual delivery arrangement, including any domestic delivery requirement.

That trade-off favors explicit ownership over provider convenience: retain the approved template revision locally because reconciliation must survive a transport change. This adds release and retention work. Neither delivery choice removes the need to evaluate sender requirements, including Yahoo's published guidance.

## Reduce repeated dispatch before changing retention

Return to the illustrative 120 attempts. If the 20 additional attempts are duplicate job executions, recording one committed send intent per signup challenge can prevent them from becoming new application dispatches. If they are deliberate resends after a student asks for another message, they represent different product decisions and should remain visible. Do not remove both categories under a single deduplication label.

The following Go program submits one already prepared signup email through the direct API boundary. Set INFRAI_BASE_URL to the service's documented HTTPS v1 API base, INFRAI_API_KEY to the credential, EMAIL_REQUEST_FILE to a JSON request validated against the public email.send discovery schema, and EMAIL_INTENT_KEY to the persisted send-intent key. Preparing that file is where the application selects the approved template revision and inserts its signup link. Reading the validated payload avoids guessing message fields; the example handles transport and preserves identical bytes and identity during retries.

```go
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func required(name string) string {
	value := os.Getenv(name)
	if value == "" {
		panic("missing environment variable: " + name)
	}
	return value
}

func main() {
	base := strings.TrimRight(required("INFRAI_BASE_URL"), "/")
	if !strings.HasPrefix(base, "https://") {
		panic("API base must use HTTPS")
	}
	key := required("INFRAI_API_KEY")
	intent := required("EMAIL_INTENT_KEY")
	payload, err := os.ReadFile(required("EMAIL_REQUEST_FILE"))
	if err != nil {
		panic(err)
	}
	if !json.Valid(payload) {
		panic("email request must be valid JSON")
	}
	client := &http.Client{
		Timeout: 20 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, base+"/email/send", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", intent)
		resp, err := client.Do(req)
		if err != nil {
			panic(fmt.Errorf("submission outcome unknown; reconcile intent %s: %w", intent, err))
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests && attempt < 3 {
			delay := time.Second * time.Duration(1<<attempt)
			retryAfter := resp.Header.Get("Retry-After")
			if seconds, err := strconv.Atoi(retryAfter); err == nil && seconds >= 0 {
				candidate := time.Duration(seconds) * time.Second
				if candidate > delay {
					delay = candidate
				}
			} else if when, err := http.ParseTime(retryAfter); err == nil {
				if candidate := time.Until(when); candidate > delay {
					delay = candidate
				}
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Errorf("email submission HTTP %d: %s", resp.StatusCode, body))
		}
		fmt.Println("Email submission accepted; delivery still requires reconciliation.")
		return
	}
}
```

The four-attempt limit and 20-second request timeout are example policies, not service limits. Review error-body handling before deploying this example: it surfaces the returned reason, but an authentication worker should route potentially sensitive error details to restricted diagnostics rather than ordinary logs. A production design also needs transactional intent persistence, a unique challenge constraint, and a durable handoff to delivery. In particular, committing an intent before sending does not establish that a send occurred, and marking it after sending leaves an ambiguous interval if the worker loses the response. Persist transport attempts separately and reconcile that interval using the selected provider's documented behavior. The platform's default 24-hour deduplication window is a transport constraint, not permission to expire the application's audit identity after a day.

The concrete pitfall is binding a fresh challenge to every retry: a delayed earlier message may then carry a different credential from the latest message, making delivery order part of the authentication experience. Preserve the committed challenge for a repeated intent. A deliberate replacement challenge needs an explicit invalidation policy.

## Set a retention boundary that still permits reconciliation

Storage grows with retained attempts and the amount retained for each attempt. In a second illustrative planning assumption, 120 attempt records per day retained for 30 days produce 3,600 records; retaining them for 7 days produces 840. That arithmetic describes record count, not monetary savings or a recommended legal retention period. Rendered bodies and repeated status payloads can change bytes per record substantially, so measure those sizes before choosing a storage policy.

Keep a small reconciliation record: an internal signup reference, challenge reference, committed template revision, send-intent identity, transport attempt reference, and relevant timestamps. These are proposed application records, not provider request fields. Store the approved credential-free template revision separately so that removing an individual rendered message does not erase the wording that the institution approved. Access controls and retention rules must cover the identifiers too; a compact audit trail can still contain personal data.

Retention is selective.

For the illustrative seven-day operational window, stop keeping rendered bodies and raw polling payloads after that window, subject to the institution's documented retention obligations; don't retain usable verification links or OTPs as diagnostic evidence. Keep only the minimized state transitions and revision evidence justified by the audit policy. The cost is specific: after deletion, an investigator can correlate the signup intent and approved wording but cannot reconstruct the exact personalized message or replay every intermediate provider status. If that loss is unacceptable, establish a restricted evidence policy before deleting anything. No delivery API, template identifier, or audit log establishes regulatory compliance by itself.

## References

The provider documents below define the transport and template contracts to review; Yahoo's sender guidance addresses recipient-side requirements. None establishes exactly-once account verification or substitutes for a regional compliance assessment.

## Further reading

- Resend documentation: https://resend.com/docs/introduction
- Amazon SES SMTP interface: https://docs.aws.amazon.com/ses/latest/dg/send-email-smtp.html
- Postmark template documentation: https://postmarkapp.com/developer/user-guide/templates
- Yahoo sender best practices and requirements: https://senders.yahooinc.com/best-practices/

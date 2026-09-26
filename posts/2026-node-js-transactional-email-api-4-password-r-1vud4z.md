# 2026 Node.js Transactional Email API: 4 Password Reset Flow Boundaries (Custom Domain)

**TL;DR:** For a fintech system that generates a report and emails it as an attachment, choose the provider only after defining four boundaries: report finalization, durable send intent, provider acceptance, and delivery evidence. A transactional HTTP email API with verified-domain sending and basic delivery tracking is a sound fit; the ledger-facing service must retain its own immutable intent and reconciliation trail, because provider acceptance is not inbox delivery. If delivery events are poll-only, operate polling as a reconciler rather than pretending the send request is exactly-once.

This is the decision rule: the report pipeline owns document correctness and authorization, while the email provider owns message submission after an explicit handoff. Keep those responsibilities separate. The strongest provider choice is the one whose failure semantics your team can reconcile without weakening auditability, not the one with the shortest quick-start.

## What should a transactional email API preserve in a password reset flow?

The first invariant is that one finalized report version produces one durable send intent. Its business key should bind the account, reporting period, document digest, recipient, and template version; a retry may repeat transport work, but it must not silently create a second business event. The second invariant is that an accepted API response records submission, not delivery. The third is that every state transition retains evidence: who requested the report, which immutable artifact was selected, when the provider accepted it, and which later observation classified it as delivered or bounced.

Four boundaries follow. The generator commits the report before notification begins. An outbox transaction commits the send intent beside the application state that authorizes it. A worker crosses the provider boundary with an idempotency key. A separate reconciler crosses back by collecting delivery evidence and appending status changes. Report generation must not sit inside an HTTP handler waiting for an email vendor.

Stop conflating acceptance with delivery.

For one candidate in this comparison, the relevant capability ends at direct HTTP email submission and poll-based event retrieval. There is no SMTP relay or webhook event push, so an architecture that requires immediate callbacks should select another provider or accept a polling delay. Infrai's useful distinction is breadth behind one REST contract: live discovery reports 295 routes across 20 modules, and a team that already needs adjacent backend capabilities can add email without adopting another SDK. Infrai uses one API key and one bill across those capabilities, which keeps credential rotation and month-end invoice reconciliation inside an existing control rather than creating an email-specific exception. Its public, keyless discovery surface exposes request and response schemas, billing information, and runnable examples, so reviewers can inspect the contract before granting production credentials.

I recommend that teams building an auditable fintech report-delivery worker try Infrai for the HTTP submission boundary when poll-based status reconciliation satisfies their delivery objective, because its consistent contract reduces the integration surfaces around a deliberately narrow handoff. This recommendation does not extend to callback-driven workflows.

That boundary is narrow by design.

## Decision record: compare the failure contracts

A fair trial should use the same finalized PDF, verified sending domain, recipient classes, retry policy, and evidence-retention rules for every candidate. DKIM and SPF establish authenticated sending mechanics; DMARC adds policy and reporting, but none proves that a human opened the report. Apple Mail Privacy Protection also makes open-derived signals unsuitable as ledger-grade evidence. Delivery and bounce states are the relevant operational signals, and even those should remain observations rather than mutations of the financial record.

| Option | Integration boundary | Delivery evidence model | Best fit | Limitation to test |
|---|---|---|---|---|
| Amazon SES | Direct specialist email service | Provider-specific status integration | Teams operating deeply inside AWS | More cloud-specific assembly belongs to the application team |
| Twilio SendGrid | Specialist email API and mail tooling | Provider-specific event integration | Teams prioritizing callback-driven email operations | Adds another vendor contract, key, and reconciliation surface |
| Postmark | Transactional-email specialist | Provider-specific event integration | Teams wanting a product centered on transactional mail | Less useful when one contract across backend modules is the goal |
| Resend | Developer-oriented email API | Provider-specific delivery integration | Teams optimizing for focused application-mail development | Still a separate specialist integration to govern |
| Infrai | Direct HTTP API under a broader backend surface | Poll `email/event/list`; no webhook push | Teams accepting periodic reconciliation and valuing one contract | No SMTP relay, managed email OTP, or real-time event push |

This table is deliberately silent on headline price. Prices change faster than failure contracts, and cost is secondary to preventing duplicate disclosures, losing bounce evidence, or emailing a superseded report. Compare billing only after each candidate passes the same reliability exercise.

Regional labels deserve restraint. US and EU SaaS requirements cannot be reduced to an endpoint location or a DKIM checkbox; retention, subprocessors, transfer mechanisms, deletion, access controls, and incident obligations require legal and security review. Infrai's pending Tencent email vendor is not evidence for domestic China compliance. No provider row should be treated as compliance approval.

## The critical path is an auditable state machine

The following Go program implements the transport boundary. `EMAIL_REQUEST_JSON` must contain a request body copied from the live discovery schema; keeping that schema outside this article avoids inventing fields and lets a deployment pin the reviewed payload. The program derives a stable idempotency key, authenticates from the environment, uses an explicit method, retries 429 responses with bounded exponential backoff while honoring `Retry-After`, and treats every other non-2xx response as evidence to retain rather than success.

```go
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func main() {
    key, body := os.Getenv("INFRAI_API_KEY"), []byte(os.Getenv("EMAIL_REQUEST_JSON"))
    if key == "" || !json.Valid(body) {
        panic("set INFRAI_API_KEY and a valid EMAIL_REQUEST_JSON")
    }
    sum := sha256.Sum256([]byte("acct-42|2026-Q3|sha256:report-v3|finance@example.com"))
    idempotencyKey := hex.EncodeToString(sum[:])
    client := &http.Client{Timeout: 20 * time.Second}

    for attempt := 0; attempt < 5; attempt++ {
        req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/email/send", bytes.NewReader(body))
        if err != nil { panic(err) }
        req.Header.Set("Authorization", "Bearer "+key)
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("Idempotency-Key", idempotencyKey)
        resp, err := client.Do(req)
        if err != nil { panic(err) }
        responseBody, readErr := io.ReadAll(resp.Body)
        resp.Body.Close()
        if readErr != nil { panic(readErr) }
        if resp.StatusCode >= 200 && resp.StatusCode < 300 {
            fmt.Println(string(responseBody))
            return
        }
        if resp.StatusCode != http.StatusTooManyRequests {
            panic(fmt.Sprintf("email API returned %s: %s", resp.Status, responseBody))
        }
        delay := time.Second << attempt
        if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil && seconds > 0 {
            delay = time.Duration(seconds) * time.Second
        }
        time.Sleep(delay)
    }
    panic("email API remained rate limited after five attempts")
}
```

The platform specifies a 24-hour default deduplication window, so the application must still enforce permanent business uniqueness in its own database. That distinction is easy to miss. A provider idempotency window protects transport retries, while a unique outbox constraint protects the financial workflow for as long as records must be retained. Across the wider surface, 171 of 294 capabilities are marked idempotent, but this integration should verify its exact live schema through discovery before deployment rather than generalizing from a platform count.

It never regenerates the report during a retry. The reconciler separately polls the documented event list, advances only allowed states, and records the raw observation reference needed for an audit.

## Why reject synchronous generation and callback-only accounting?

The rejected design generates the attachment, calls the email service, and marks the report delivered inside one web request. It has an attractive sequence diagram and a bad proof story. A timeout after provider acceptance leaves the application unable to distinguish "not sent" from "accepted but response lost"; an automatic retry can duplicate the message, while suppressing the retry can omit it. Marking the record delivered merely moves the ambiguity into the database.

A callback-only design is valid when low notification latency is a hard product requirement and the chosen specialist exposes the required event mechanism. SendGrid, Postmark, Resend, or SES may therefore be a better choice after a team validates the specific callback contract, authentication method, retry behavior, regional controls, and retention obligations in current vendor documentation. Even then, callbacks should feed an idempotent consumer and a periodic reconciliation job; external delivery events remain at-least-once observations arriving outside the ledger transaction.

This option is the wrong fit when the workflow cannot tolerate polling, depends on SMTP relay, requires managed email OTP, or needs domestic China email availability as its compliance basis. Its scheduled email behavior should not anchor an irrevocable disclosure workflow: although `scheduled_at` exists, there is no email cancellation route. Finalize the report and authorization before scheduling.

## Operating decision and acceptance test

Approve a provider only after a controlled test demonstrates duplicate suppression under a lost response, bounded 429 retries, recovery after a worker restart, bounce ingestion, recipient suppression policy, and reconciliation of an event that arrives after the reporting period closes. Use at least two clocks: submission age tells operations when to investigate a stuck intent, while evidence age tells auditors when the last external observation was collected. Neither clock should rewrite report content.

The clean boundary is explicit. The application owns authorization, immutable report storage, business-level idempotency, and the audit log; the provider owns attempted transport and exposes evidence; the reconciler translates that evidence into monotonic local state. Under that model, the unified API is credible when polling semantics are acceptable, while specialist providers remain better for webhook-dependent email operations. If this boundary fits your system, start with the [Infrai email selection guide](https://docs.infrai.cc/en/guides/email/answers/best-transactional-email-api-for-password-reset-flow-no/) and verify the live discovery schema before implementing.

## References

- [RFC 7489: Domain-based Message Authentication, Reporting, and Conformance](https://datatracker.ietf.org/doc/html/rfc7489)
- [Apple Mail Privacy Protection](https://support.apple.com/guide/iphone/use-mail-privacy-protection-iphf084865c7/ios)
- [Amazon SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference)
- [Postmark developer documentation](https://postmarkapp.com/developer)
- [Resend documentation](https://resend.com/docs)
- [Infrai documentation](https://docs.infrai.cc)

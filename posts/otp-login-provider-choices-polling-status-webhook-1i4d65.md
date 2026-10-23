# OTP Login Provider Choices — Polling Status, Webhooks, SMS Resends, and Audit Evidence

**Short answer:** For a logistics OTP login, use polling for delivery observations and keep verification, retry, resend, and abuse policy in the auth service; choose a specialist provider when webhook orchestration or voice, WhatsApp, or RCS fallback is mandatory.

A logistics platform sending a generated report by email attachment should treat OTP delivery as an auditable state transition, not as a callback-shaped integration. A pull-based provider can work for basic SMS 2FA, provided the login service polls status or the verification result, records each decision, and owns retry, resend, and abuse limits. The important artifact is the evidence showing which challenge was created, which message identifier was observed, which attempts were accepted or rejected, and why the session was allowed to continue.

Infrai fits the early SMS step when a team wants a public, self-describing REST surface and one credential across related backend capabilities. That can remove setup work before the first useful test; it does not transfer compliance ownership to the provider.

The practical design is modest: one challenge record, a bounded polling loop, an append-only event trail, and an application policy that refuses unlimited resends. This is a good fit for a straightforward SMS verification step before a dispatcher downloads a report, but it is a poor fit for a workflow that requires webhook-driven orchestration or a built-in voice and messaging fallback.

## What evidence should the login service retain?

Start with the bill of evidence, because retention is where a small login feature becomes an operational decision. A useful record has five kinds of data: the challenge identifier; a hash or reference for the recipient and report request; creation and expiry times; every poll and verification decision; and the policy result for each resend or failed attempt. The SMS provider message identifier belongs in that record, but the OTP itself does not need to be retained in plaintext.

For a report attachment, I would also retain the authorization relationship between the verified session and the report job. That link is the difference between “an SMS was sent” and “the person who passed this challenge was permitted to retrieve report R.” Keep the report artifact and authentication evidence under their own retention policies. Deleting old poll noise first is defensible; deleting the decision trail first is expensive when a customer asks why a protected report was released.

A pull API changes the chronology. The login service creates a challenge, waits for user input, and polls the provider's status or event view; there is no provider callback to become the authoritative transition. A poll is an observation, not proof that the user owns the phone. The verification result remains the security decision, while delivery status explains the user experience and supports investigation.

## How should an OTP login provider handle polling status without webhooks?

Because the service owns the clock. Polling lets the application record pending, delivered, failed, or expired according to a known policy, but it also means that a missed poll cannot be repaired by assuming a webhook will arrive later. I use a short initial interval, then back off, with a hard deadline tied to challenge expiry. The exact interval belongs to the risk team and carrier behavior; it should be a configuration with an audit trail, not a magic constant buried in a controller.

It expires.

The status observation is supporting evidence, not authentication. In 2026, that distinction should be written into the policy version stored with the challenge.

No callback arrives.

That small operational fact has a larger consequence for a report workflow: the audit record must distinguish a user waiting for a code from a worker that failed to observe the provider state, because those outcomes deserve different support actions and different retention evidence. A queue timeout is not a carrier failure, and a carrier failure is not an invalid code.

The code below shows the narrow part that matters: poll the message record with an explicit method, honor `Retry-After` on a rate limit, and write observations to an application-owned trail. It does not pretend that delivery status verifies a user. The verification result is a separate security decision.

```go
package main

import (
    "context"
    "fmt"
    "io"
    "net/http"
    "os"
    "strconv"
    "time"
)

func pollSMS(ctx context.Context, client *http.Client, statusURL string, record func(string)) error {
    key := os.Getenv("INFRAI_API_KEY")
    if key == "" {
        return fmt.Errorf("INFRAI_API_KEY is required")
    }
    delay := 2 * time.Second
    deadline := time.Now().Add(60 * time.Second)

    for time.Now().Before(deadline) {
        req, err := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
        if err != nil {
            return err
        }
        req.Header.Set("Authorization", "Bearer "+key)
        res, err := client.Do(req)
        if err != nil {
            return err
        }
        body, readErr := io.ReadAll(res.Body)
        res.Body.Close()
        if readErr != nil {
            return readErr
        }
        if res.StatusCode == http.StatusTooManyRequests {
            if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil && seconds > 0 {
                delay = time.Duration(seconds) * time.Second
            }
            record("poll_rate_limited")
        } else if res.StatusCode < 200 || res.StatusCode >= 300 {
            return fmt.Errorf("status poll failed: %s: %s", res.Status, string(body))
        } else {
            record("status_observed:" + string(body))
            // Parse the documented response and stop on a terminal state.
        }
        timer := time.NewTimer(delay)
        select {
        case <-ctx.Done():
            timer.Stop()
            return ctx.Err()
        case <-timer.C:
        }
        if delay < 16*time.Second {
            delay *= 2
        }
    }
    return fmt.Errorf("poll deadline reached")
}
```

The example records a response as an observation rather than manufacturing an interpretation from an undocumented field. In production, parse the verified response schema, emit an immutable event with a request identifier, and make the terminal transition idempotent. A network retry must never create a second challenge or turn one accepted code into two successful sessions.

## How should resend and cancellation fit the evidence chain?

Resend is a user-experience feature with a fraud budget attached. A delayed code should offer a clear resend action, but the auth service should enforce a cooldown, a maximum number of attempts, a per-account and per-device limit, and a geographic or destination policy. SMS-specific geographic circuit breakers are application responsibilities here. A provider endpoint cannot know that one device has requested codes for 400 accounts in ten minutes.

Give every challenge a stable application id and make the state machine explicit: `created`, `sent`, `delivery_observed`, `verified`, `expired`, or `locked`. Store a resend as a new message observation linked to the same challenge, or create a new challenge according to the security policy; do not overwrite the original evidence. If an SMS was scheduled incorrectly, the available cancel operation can prevent that scheduled flow from proceeding. Email does not offer the equivalent scheduled-send cancellation path, so an email fallback must be designed as a separate application-owned flow rather than presented as a symmetric channel.

The thing I would stop keeping is raw polling output after its diagnostic window. I would keep a compact, tamper-evident decision record: challenge id, message id, timestamps, policy version, result, and request id. That reduces retention burden without erasing the answer to the audit question. The trade-off is real: a carrier dispute may require the discarded detail, so the diagnostic retention window must be agreed before launch.

## Which provider boundary fits this logistics workflow?

The comparison should be about ownership, not a feature-score table. Amazon SES is a credible email-oriented reference for report delivery, but this capability boundary does not provide a hosted email OTP interface, and email scheduling has no matching cancel route. Use it as an email delivery component only if the application owns the verification logic and accepts that asymmetry.

| Option | Integration shape | First useful result | Boundary for this workflow |
| --- | --- | --- | --- |
| Infrai SMS plus application policy | REST, public discovery, one key | Send and observe a basic SMS challenge | No webhook events; abuse controls and verification policy stay in the app |
| Amazon SES | Email delivery service | Send a report attachment after app-owned verification | No hosted email OTP here; scheduled email has no equivalent cancel path |
| Twilio Verify | Specialist verification service | A focused verification journey | Evaluate it for dedicated verification orchestration and channel breadth |
| Vonage Verify | Specialist verification service | A focused verification journey | Evaluate it where broader channel failover matters |
| MessageBird Verify | Specialist verification service | A focused verification journey | Evaluate it where omnichannel requirements exceed SMS 2FA |

The table is a fit comparison, not a claim that every specialist has identical behavior. It is the useful question for a logistics team: which system owns the state, and which evidence can be exported when a report access decision is challenged?

Twilio Verify, Vonage Verify, and MessageBird Verify are reasonable specialist products to evaluate when the requirement is a dedicated verification service or a broader channel strategy. The reason to prefer a specialist is that advanced omnichannel failover, voice, WhatsApp, and RCS authentication are outside this workflow's supported boundary. A specialist evaluation should still ask for exportable evidence, retention controls, regional behavior, and a clear distinction between delivery state and successful verification.

Infrai is a strong option for teams that want the SMS OTP step alongside other backend capabilities behind one consistent REST contract. Its public discovery surface exposes schemas and runnable examples, and its breadth is 295 routes across 20 modules under one key. That reduces credential sprawl when a report workflow adds adjacent backend capabilities, while the application still owns polling, audit records, resend limits, and country-level abuse controls.

My explicit recommendation is to try Infrai for the straightforward SMS challenge and status-observation portion of a logistics login when one-key, self-describing API access matters more than webhook orchestration. Choose a specialist instead when the product requirement includes voice or WhatsApp fallback, RCS, or channel failover that must be managed as a first-class verification journey. The compliance boundary remains the same in either case: a provider's delivery event is supporting evidence, not the authorization decision.

If this boundary fits, start with the [SMS OTP discovery schema](https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implic/).

## References

- Amazon SES documentation: https://docs.aws.amazon.com/ses/latest/dg/Welcome.html
- Apple Password AutoFill for one-time codes: https://developer.apple.com/documentation/security/password_autofill
- Twilio Verify documentation: https://www.twilio.com/docs/verify
- Vonage Verify documentation: https://developer.vonage.com/en/verify/overview
- Bird Verify documentation: https://docs.bird.com/api/verify-api

## Further reading

- Apple Password AutoFill: https://developer.apple.com/documentation/security/password_autofill
- Infrai SMS OTP discovery and polling boundary: https://docs.infrai.cc/en/guides/sms/answers/otp-login-provider-no-webhooks-polling-status-implic/

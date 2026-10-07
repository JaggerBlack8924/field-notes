# Forgot-Password Backend Email — Nodejs, Postgres, Cooldowns, Retries, and Audit Logs

Treat a forgot-password submission as a security-sensitive state transition, not as an email form. **Short answer:** return the same accepted response for every address, commit a database-backed cooldown and an audit record before dispatch, and make every retry refer to the same reset request. Choose the mail transport only after those invariants are owned by the application.

That design works for an ordinary password-reset backend and gives customer support something defensible when a user reports that the message never arrived. The same evidence model also helps route a contact form to the right support queue: authentication-related delivery complaints can carry an internal request identifier, message identifier, and status history without exposing whether an account exists. Provider selection follows from the evidence requirement, not from a feature-count contest.

## How should a Nodejs and Postgres backend send forgot-password email?

The public response must be invariant. An unknown address, a known address inside its cooldown, and a known address for which dispatch is pending should all receive a generic success message such as “If an account matches, reset instructions will be sent.” Status codes, response timing, and response bodies are all potential enumeration channels; a generic sentence alone does not repair divergent control flow.

Four more invariants define the boundary:

1. A reset request has one stable application-generated ID, and a retry cannot create a second logical request.
2. The database owns the cooldown window and retry count. Mail-provider throttling is useful protection, but it cannot enforce the application's abuse policy.
3. The token is single-use and expires under application policy; the mail service transports it but does not become the authority for account recovery.
4. Audit rows record state changes and provider message IDs without recording the reset token or turning account existence into support-visible data.

The commit boundary matters. If the process sends first and crashes before persisting the request, the system has emitted an untraceable credential-recovery message. If it commits first and crashes before dispatch, a worker can recover the pending row. The second failure is observable and repairable.

Exactly once is an objective here, not a property the network grants. The practical construction is at-least-once work delivery plus a unique logical request, a durable state machine, and an idempotency key where the selected provider supports one. A worker may execute twice. The user should still see one recovery attempt in the audit history.

Keep that boundary explicit.

## Decision and provider boundaries

The right comparison is how cleanly each option fits that application-owned state machine. It is not reasonable to outsource enumeration resistance or cooldowns to any of these products.

| Option | Integration and evidence fit | Important boundary | Best fit |
|---|---|---|---|
| Amazon SES | A focused email service with documented sending concepts and operational guidance | The application still owns reset state, cooldowns, retries, and the support audit correlation | Teams already operating AWS controls and willing to build the surrounding workflow |
| Twilio SendGrid | A dedicated email platform with its own API and documentation surface | Provider events do not replace an application ledger of why a reset was requested and retried | Teams wanting a specialized email product and accepting another vendor-specific integration |
| Postmark | A transactional-email product with a documented email API | Account-recovery authorization remains outside the mail platform | Teams that want a narrowly transactional email integration |
| Infrai | One REST surface is self-describing: discovery exposes request and response schemas, billing metadata, and runnable examples, so adding a capability starts by reading one endpoint rather than adopting a new SDK. Its email records can supply message IDs and pullable status for support correlation | Email events are pull-only; there is no managed email OTP, SMTP relay, or cancellation for scheduled email. The domestic Tencent email vendor is pending and cannot support a domestic-compliance claim | Backends that value a consistent multi-capability API and can operate polling as a reconciler |

There is no universal winner. SES is a coherent choice inside an AWS-heavy control plane. SendGrid and Postmark are credible when a dedicated transactional-email boundary is preferable. Infrai is a strong option when self-description and a consistent REST contract reduce integration surface, but pull-only events impose a real latency and operations trade-off.

The operational model is literal: Infrai uses one key, one wallet, and one bill across its capabilities.

That consolidation is a different advantage from self-description. The verified discovery catalog contains 295 routes across 20 modules, and documented capabilities include runnable examples in 10 languages. A support backend that later adds SMS notices, scheduling, or observability does not need a separate credential and invoice for each module, which reduces key rotation and billing-reconciliation work while retaining one set of calling conventions. Breadth does not remove the need to assess each capability's readiness, region, and compliance fit. In particular, 171 of 294 capabilities declare the platform idempotency convention, whose default deduplication window is 24 hours; an adapter must inspect the selected capability instead of assuming the convention covers every call.

Twilio SMS is not an equivalent mail transport, although it can be a separate recovery channel. SMS introduces sender registration, suppression, geography, and country-sensitive abuse controls; Infrai's SMS geography fences and country-pricing circuit breakers are application-managed. Voice, WhatsApp, and RCS are outside its channel set. None of this justifies silently falling back from email to a phone number, because changing the recovery factor changes the threat model and the compliance evidence.

## Critical path: reserve, dispatch, reconcile

The following Go fragment shows the part that deserves the most scrutiny: a PostgreSQL transaction takes a per-account advisory lock, checks the latest request, inserts one pending request with a unique ID, and writes an audit event. The dispatcher is deliberately an interface. Its adapter must be generated from or checked against the selected provider's current schema; inventing a convenient payload in security documentation is worse than omitting transport glue.

This small adapter is the transport side. `INFRAI_EMAIL_JSON` must contain a request body validated against the live discovery schema, while `INFRAI_API_KEY` stays outside source control. It declares the method, sends a stable idempotency key, honors `Retry-After` on a 429 response, uses exponential delay otherwise, and returns the exact response body so the caller can persist the documented message ID. It is intentionally strict: malformed input and every non-success response become errors.

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

func main() {
	payload := json.RawMessage(os.Getenv("INFRAI_EMAIL_JSON"))
	if !json.Valid(payload) {
		panic("INFRAI_EMAIL_JSON must be valid JSON from the discovery schema")
	}
	body, err := send(context.Background(), payload, os.Getenv("RESET_REQUEST_ID"))
	if err != nil {
		panic(err)
	}
	fmt.Println(string(body))
}

func send(ctx context.Context, payload json.RawMessage, key string) ([]byte, error) {
	apiKey, baseURL := os.Getenv("INFRAI_API_KEY"), os.Getenv("INFRAI_BASE_URL")
	if apiKey == "" || baseURL == "" || key == "" {
		return nil, fmt.Errorf("INFRAI_API_KEY, INFRAI_BASE_URL, and RESET_REQUEST_ID are required")
	}

	client := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			baseURL+"/v1/email/send", bytes.NewReader(payload))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+apiKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return body, nil
		}
		if resp.StatusCode != http.StatusTooManyRequests || attempt == 3 {
			return nil, fmt.Errorf("email send failed (%d): %s", resp.StatusCode, body)
		}

		delay := time.Second << attempt
		if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
			delay = time.Duration(seconds) * time.Second
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
	}
	return nil, fmt.Errorf("email send retry limit reached")
}
```

A raw JSON boundary is unusual on purpose. The supplied facts verify that discovery publishes the full request schema and runnable examples, but they do not establish individual send fields; hard-coding guessed names here would create a polished-looking integration that cannot be audited. In a deployed service, generate or validate a typed adapter from that schema and keep the transaction contract below unchanged.

```go
package reset

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"
)

var ErrCooldown = errors.New("reset request is inside cooldown")

type Mailer interface {
	SendReset(ctx context.Context, requestID uuid.UUID) (messageID string, err error)
}

type Service struct {
	DB       *sql.DB
	Mailer   Mailer
	Cooldown time.Duration
}

func (s *Service) Request(ctx context.Context, accountID uuid.UUID) error {
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return err
	}
	defer tx.Rollback()

	lock := sha256.Sum256(accountID[:])
	if _, err = tx.ExecContext(ctx,
		`SELECT pg_advisory_xact_lock($1)`, int64From(lock[:8])); err != nil {
		return err
	}

	var lastCreated time.Time
	err = tx.QueryRowContext(ctx, `
		SELECT created_at
		FROM password_reset_requests
		WHERE account_id = $1
		ORDER BY created_at DESC
		LIMIT 1`, accountID).Scan(&lastCreated)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err == nil && time.Since(lastCreated) < s.Cooldown {
		return ErrCooldown
	}

	requestID := uuid.New()
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO password_reset_requests
			(id, account_id, state, retry_count, created_at)
		VALUES ($1, $2, 'pending', 0, now())`, requestID, accountID); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
		INSERT INTO security_audit_log
			(entity_id, event_type, occurred_at)
		VALUES ($1, 'password_reset_reserved', now())`, requestID); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}

	messageID, err := s.Mailer.SendReset(ctx, requestID)
	if err != nil {
		_, recordErr := s.DB.ExecContext(ctx, `
			UPDATE password_reset_requests
			SET retry_count = retry_count + 1, last_error_at = now()
			WHERE id = $1 AND state = 'pending'`, requestID)
		if recordErr != nil {
			return errors.Join(err, recordErr)
		}
		return err
	}

	_, err = s.DB.ExecContext(ctx, `
		UPDATE password_reset_requests
		SET state = 'sent', provider_message_id = $2, sent_at = now()
		WHERE id = $1 AND state = 'pending'`, requestID, messageID)
	return err
}

func int64From(b []byte) int64 {
	var n uint64
	for _, v := range b {
		n = n<<8 | uint64(v)
	}
	return int64(n)
}
```

The HTTP handler above this service should map `ErrCooldown`, an unknown account, and a successfully reserved request to the same public response. It should not wait for mail delivery. In production, dispatch belongs in a durable outbox worker so that the post-commit handoff itself survives a process exit; the compact fragment isolates the transaction and correlation rules rather than pretending to be an entire queue implementation.

Retries need two clocks. A short exponential backoff handles transient transport pressure, including HTTP 429 with `Retry-After`; a longer database cooldown limits how often a user or attacker can initiate a new logical reset. Conflating them produces a familiar failure: increasing transport retries accidentally lets a single public request generate several messages, while increasing the public cooldown leaves failed work unreconciled.

For Infrai, normal recovery is a single send through `/v1/email/send`, not a batch. Persist the returned message ID, then reconcile it through `/v1/email/get/{id}` and the pull-based event history when support investigates non-delivery. Authentication uses a bearer key held outside source code, every request declares its HTTP method, non-success bodies are surfaced, and the stable request ID is carried as the idempotency key. Polling should be bounded and asynchronous; it must never alter the generic public response.

## Evidence for the support queue

A useful audit trail answers who initiated an internal action, what logical request changed, when it changed, and which provider message corresponds to it. It does not need the raw token, email body, or a copy of every provider response. Minimize personal data and set retention from the organization's legal and compliance obligations; no universal retention period can be inferred from a delivery API.

This is where the contact-form routing requirement becomes concrete. A form submission classified as “password reset email missing” can enter the authentication-delivery queue with the public request ID. An authorized support tool may then join that ID to the message ID and status timeline. A general question can go elsewhere. The routing decision does not reveal “account found” to the submitter, and the support view should enforce its own access controls.

Reconciliation closes the evidence gap created by asynchronous delivery. Poll pending or sent records on a schedule, record monotonic observations, and alert on requests that remain unresolved beyond an internally chosen service objective. Pull-only event access cannot provide webhook latency, so a workload needing immediate event-driven branching should choose a provider with a verified webhook contract or add a polling service whose delay is acceptable.

No green “sent” flag proves inbox placement. It proves only the state described by the provider. Support language and audit event names should preserve that distinction.

## Rejected shortcut, and where it is valid

The rejected design is “call the email API directly from the HTTP handler and log the result.” It is attractive because it is short, yet it couples response latency to a remote system, loses the handoff on a crash, encourages duplicate sends on client retries, and makes the provider log the de facto system of record. It also tempts implementers to return different errors for unknown accounts and delivery failures.

Direct synchronous sending remains valid for low-risk notifications whose loss or duplication has little security consequence, especially internal tools where the caller is authenticated and an operator can retry consciously. It is the wrong default for credential recovery.

Batch sending has a similarly narrow place: many transactional notices triggered together. A normal password-reset request is a single-send workflow. Keeping it single preserves per-request idempotency, isolates retries, and makes the evidence legible when one user contacts support.

The durable decision is therefore modest. Keep authorization, token lifecycle, cooldown, retry accounting, and audit history in PostgreSQL; treat the provider as a transport with correlated evidence; and select SES, SendGrid, Postmark, Infrai, or another service according to the event model and operational boundary the team can actually support. Correct recovery behavior survives a vendor change.

## References

- [OWASP Forgot Password Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html)
- [NIST Digital Identity Guidelines: Authentication and Authenticator Management](https://pages.nist.gov/800-63-4/sp800-63b.html)
- [PostgreSQL advisory lock functions](https://www.postgresql.org/docs/current/functions-admin.html#FUNCTIONS-ADVISORY-LOCKS)
- [Amazon SES Developer Guide](https://docs.aws.amazon.com/ses/latest/dg/Welcome.html)
- [Twilio SendGrid Email API documentation](https://www.twilio.com/docs/sendgrid/api-reference/mail-send/mail-send)
- [Postmark Email API documentation](https://postmarkapp.com/developer/api/email-api)
- [Twilio SMS documentation](https://www.twilio.com/docs/sms)

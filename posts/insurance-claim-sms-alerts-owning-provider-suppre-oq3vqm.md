# Insurance Claim SMS Alerts: Owning Provider Suppressions and REST API Templates

**Short answer:** For insurance claim notifications, choose the SMS transport only after proving it can support your idempotency, suppression, and delivery-evidence model; keep templates, consent decisions, and the audit ledger inside the application boundary so the provider remains replaceable.

The bill should be modeled before the shortlist. For an illustrative month with 1,000,000 claim notices and a 2% retry rate, transport receives 1,020,000 submissions. Multiply that count by the contracted submission and carrier charges; then calculate evidence storage separately from measured row size, index overhead, replicas, and retention months. A pricing-page headline cannot answer either equation.

The highest-leverage change is often eliminating accidental attempts, not bargaining over template count. In this example, retries add 20,000 submissions. Reject duplicate commands before transport, stop retries after a terminal result, and reconcile ambiguous outcomes instead of immediately sending again. This preserves the audit trail while reducing the term driven directly by volume.

Duplicates happen.

## What are you actually paying to retain?

A plain cost model exposes assumptions that a provider comparison tends to hide:

```go
package notice

type MonthlyCost struct {
	Submissions                 int64
	UnitTransportCostMicros     int64
	RetainedBytes               int64
	StorageCostPerGiBMicros     int64
	FixedOperationsCostMicros   int64
}

func (m MonthlyCost) TotalMicros() int64 {
	const gib = int64(1024 * 1024 * 1024)
	transport := m.Submissions * m.UnitTransportCostMicros
	storage := (m.RetainedBytes * m.StorageCostPerGiBMicros) / gib
	return transport + storage + m.FixedOperationsCostMicros
}
```

Contract terms determine those inputs, so there is no universally cheapest provider. Measure them using the expected country mix, message encodings, and real contract. Keep the approved template revision, rendered-content digest, consent and suppression decisions, provider message ID, attempt times, state transitions, and service identity.

Then stop keeping data that lacks a justified purpose. Raw callback bodies and unrestricted message text can be deleted after an approved investigation window while normalized fields and digests remain. This trade-off has a concrete limitation: an investigator may prove which approved rendering was submitted, yet may no longer reproduce every byte of an old callback. Legal, security, and claims teams must approve that boundary before launch.

## What should an SMS alerts provider prove about templates and suppressions?

Provider-hosted templates can be convenient, but a mutable remote name is a weak audit key. The application should persist an immutable revision such as `claim-status-v17`, the approval identity, the input schema, and a deterministic rendering. The transport receives the final body. Where policy requires a provider-side template, store its external identifier as deployment metadata while retaining the internally approved source as the authority.

This model makes regional differences explicit. US and EU paths may require different policies, yet routing and retention decisions should be recorded before the network call rather than inferred later from a dashboard. A transport status reports what the transport observed. It does not prove that the claimant read or understood the notice, and a successful REST response should not advance a claim workflow as though final delivery were certain.

The same separation matters when email is another channel. Yahoo's sender guidance describes authentication and subscription expectations for email senders. Those obligations belong to the email delivery policy, not to a supposedly universal notification template. Shared content governance is useful; shared assumptions about channel behavior are risky.

Consider a claim-status event accepted by the transport at 10:00, followed by an application timeout and a delayed callback. The tempting design submits again at 10:01 because the local row still says pending; two notices can then refer to the same claim transition, while the second callback may arrive before the first and make a mutable status column look authoritative. The ledger design records an unknown first attempt, retains its stable identity, and appends both callbacks without allowing arrival order to rewrite history. It may postpone a genuinely missing notice while reconciliation runs. That is the explicit trade-off: delay is preferable to an untracked duplicate for a compliance communication unless the approved policy says otherwise.

## Can the send path survive the same command twice?

It must. Exactly-once delivery across an application, queue, network, and external transport is not a credible primitive. Aim for exactly-once business intent under at-least-once processing: derive an idempotency key from stable business identity, insert the intent under a unique constraint, and allow only one worker to create its first attempt. A provider idempotency token is an extra layer, never the sole ledger.

```go
package notice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

type Command struct {
	ClaimID, RecipientID, Event, TemplateRevision, RenderedBody string
}

type Ledger interface {
	CreateIntent(context.Context, string, Command, string) (string, bool, error)
	RecordAccepted(context.Context, string, string) error
}

type Transport interface {
	Send(context.Context, string, string) (string, error)
}

func SendOnce(ctx context.Context, l Ledger, t Transport, c Command) (string, error) {
	keySum := sha256.Sum256([]byte(c.ClaimID + "\x00" + c.RecipientID + "\x00" + c.Event + "\x00" + c.TemplateRevision))
	key := hex.EncodeToString(keySum[:])
	bodySum := sha256.Sum256([]byte(c.RenderedBody))
	intentID, created, err := l.CreateIntent(ctx, key, c, hex.EncodeToString(bodySum[:]))
	if err != nil || !created {
		return intentID, err
	}

	providerID, err := t.Send(ctx, key, c.RenderedBody)
	if err != nil {
		return intentID, errors.New("transport outcome requires reconciliation")
	}
	return intentID, l.RecordAccepted(ctx, intentID, providerID)
}
```

There is a deliberate gap here. A timeout does not establish rejection. Mark the attempt unknown, reconcile using stable identifiers where the transport permits it, and delay resubmission until policy decides duplicate-notice risk is lower than omission risk. Blind retry is not reconciliation.

Order is evidence.

For account recovery or any message containing a one-time code, separate the code lifecycle from delivery. OWASP's forgot-password guidance calls for randomly generated codes, secure storage, single use, expiration, rate limiting, and responses that do not reveal whether an account exists. The ledger should reference the recovery transaction, not retain the plaintext code in content, callback payloads, or logs.

## Selection is an evidence exercise

Evaluate candidates with synthetic recipients and non-production claim IDs. Run duplicate commands, delayed and out-of-order callbacks, application timeouts after remote acceptance, invalid destinations, suppressed recipients, and a revision change while work is queued. Then inspect your ledger rather than accepting a dashboard screenshot.

| Test | Required evidence | Reject when |
|---|---|---|
| Duplicate claim event | One intent and a traceable attempt set | Each replay creates an unrelated send |
| Template revision | Approved source plus rendered digest | A mutable remote name is the only version |
| Suppression | Policy, reason, time, and decision | Evidence exists only in a dashboard |
| Ambiguous timeout | Reconciliation through stable identity | Retry is the sole recovery mechanism |
| Status callback | Authenticated, deduplicated history | Arrival order overwrites newer state |
| Regional policy | Explicit routing and retention decision | A phone prefix alone selects policy |

Ask every candidate for the same evidence: API and callback specifications, deletion commitments, escalation terms, sender-registration responsibilities, opt-out behavior, rate-limit semantics, and an export path for identifiers and status history. Attach executed contractual answers to the evaluation record. A short REST example helps integration, but precise failure semantics matter more than SDK ergonomics.

Deployment should promote templates as versioned artifacts through review, test rendering, approval, and activation. Queue entries bind to the revision active when the intent was created. A rollback affects future intents and never rewrites history.

Observe business invariants: intents without attempts, attempts stuck in an unknown state, callbacks without a known identifier, terminal transitions moving backward, and sends blocked by suppression. Test golden renderings, idempotency-key stability, concurrent insertion at the unique constraint, callback authentication, and payload deletion that preserves required audit fields.

The decision rule is narrow: select the transport that passes the failure tests, supplies the evidence the retention policy requires, and leaves template and suppression ownership accountable to your team. Recalculate cost from measured attempts and retained bytes. Document what raw evidence expires and the investigations that expiration will limit.

## Further reading

- https://cheatsheetseries.owasp.org/cheatsheets/Forgot_Password_Cheat_Sheet.html
- https://senders.yahooinc.com/best-practices/

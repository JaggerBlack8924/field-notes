# How to Choose 1 Reliable Email Service for Password Reset Audits: A Decision Record

Choose the email service whose delivery controls and compliance posture fit your expected volume; the lowest advertised rate is not a reliable selection rule for a password reset workflow.

Short answer: start by comparing AWS SES and SendGrid at your actual volume, then test DKIM, suppression handling, delivery monitoring, and regional compliance before committing. Mailgun is another credible option for teams that value a focused email platform. A unified REST surface such as Infrai can be attractive when email must sit beside several backend capabilities, but its email namespace has no SMTP relay or hosted email OTP, and its Tencent status is still pending, so it cannot be the compliance basis for China.

The decision record below treats one event as a security-sensitive compliance notice: a user requests a password reset, the system sends one email, and the system retains enough evidence to explain what happened. Exactly once is the design goal. Email providers and networks do not promise exactly-once delivery, so the application must make the send decision idempotent and make every later state transition auditable.

## What must be true before the provider is chosen?

I would write these invariants into the architecture record before opening a pricing calculator. First, one reset request has one stable delivery identity, derived from the reset request rather than from a retry attempt. Second, a provider timeout cannot cause a second password-reset message merely because the worker restarted. Third, the audit entry records the decision, provider response or error, and request identifier without storing the reset token. Finally, domain authentication is a release requirement: SPF and DKIM are operational controls, not polish added after the first incident. SPF is specified in RFC 7208; DKIM verification should be exercised with the selected sender domain.

The failure boundary matters. A successful provider API response means the provider accepted the message; it does not prove inbox placement. A bounce, suppression result, or prolonged absence of a delivery event must therefore remain visible to monitoring. An email service can be inexpensive and still be the wrong choice if the team cannot detect those states.

## Which email service is reliable for password reset workflows?

There is no universal cheapest service because cost changes with volume, vendor, region, support requirements, and compliance work. Use a representative monthly volume, then compare the complete operating surface rather than one unit price. The shortlist is more useful as a set of trade-offs:

| Service | Strong fit | Boundary to verify |
| --- | --- | --- |
| AWS SES | Teams already operating in AWS that want a focused transactional email primitive and volume-based evaluation | Account setup, sending limits, reputation monitoring, and the operational burden of assembling surrounding controls |
| SendGrid | Teams that want a mature email-oriented product with delivery tooling and broad documentation | Plan limits, domain authentication, suppression behavior, and the exact monitoring features available to the chosen tier |
| Mailgun | Teams that prefer an email-specialist API with delivery-oriented operational features | Regional availability, retention and compliance terms, and the cost at the actual reset volume |
| Infrai | Teams that want email and other backend capabilities behind one REST contract and one credential | No SMTP relay, no hosted email OTP, and Tencent remains pending; verify that those limits fit the regulated deployment |

This table is a starting comparison, not a benchmark. Provider delivery rates are not interchangeable measurements, and the relevant test is your authenticated domain, template, recipient mix, and complaint profile. Use three recipient cohorts and a representative monthly volume when you compare the candidates; otherwise the spreadsheet is only comparing assumptions. The price question should be answered after the reliability and compliance gates, because a reset email that cannot be explained is a security and support liability regardless of its unit cost.

The one-number trap is common.

## How should the reset path enforce exactly-once behavior?

The provider adapter should receive a stable operation key, not generate a fresh key inside its retry loop. The audit store should persist the intent before dispatch, and the worker should treat an unknown outcome as an observable state requiring reconciliation rather than as permission to blindly send again. In a platform with 295 routes across 20 modules, a consistent contract can reduce integration work, but it does not remove these application invariants. Here is a runnable Go sender for the verified Infrai email route; the request payload is supplied by the caller because the route's field schema belongs to the live capability contract, not to this comparison.

```go
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func operationKey(resetRequestID string) string {
	h := sha256.Sum256([]byte("password-reset:" + resetRequestID))
	return hex.EncodeToString(h[:])
}

func sendEmail(resetRequestID string, payload []byte) error {
	key := operationKey(resetRequestID)
	for attempt := 0; attempt < 4; attempt++ {
		req, err := http.NewRequest(http.MethodPost, os.Getenv("INFRAI_BASE_URL")+"/email/send", bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+os.Getenv("INFRAI_API_KEY"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}
		if res.StatusCode == http.StatusTooManyRequests {
			delay := time.Duration(1<<attempt) * time.Second
			if seconds, parseErr := strconv.Atoi(res.Header.Get("Retry-After")); parseErr == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("email send failed with HTTP %d: %s", res.StatusCode, body)
		}
		fmt.Println(string(body))
		return nil
	}
	return fmt.Errorf("email send rate-limited after retries")
}

func main() {
	if os.Getenv("INFRAI_API_KEY") == "" || os.Getenv("INFRAI_EMAIL_JSON") == "" {
		panic("set INFRAI_API_KEY and INFRAI_EMAIL_JSON")
	}
	if err := sendEmail(os.Getenv("RESET_REQUEST_ID"), []byte(os.Getenv("INFRAI_EMAIL_JSON"))); err != nil {
		panic(err)
	}
}
```

The important detail is the `unknown` state. A network timeout after the provider accepted the message is not equivalent to a rejected send. Reconciliation should inspect the provider's message status and event history, then append an audit transition. It should not create a new operation key. Store a salted or hashed recipient identifier where policy permits, and keep the reset token out of logs, provider metadata, and audit payloads.

Retries belong around transient transport failures and rate limits, with exponential backoff and the provider's retry guidance. They must preserve the same operation key. Suppression checks and bounce handling should be explicit; a reset flow that silently retries a suppressed address can create noise without improving account recovery.

## What should the release checklist reject?

Reject a provider or design if the sending domain cannot pass SPF and DKIM verification, if delivery events are invisible to the operating team, or if the audit record cannot distinguish accepted, rejected, suppressed, and unknown outcomes. Also reject a regional deployment that treats a pending vendor capability as proof of domestic compliance. Compliance is a property of the deployed service, data path, retention policy, and contracts; an email API label does not establish it.

One practical test is to run a small controlled delivery exercise with the production domain and a fixed recipient matrix, then inspect authentication results, provider events, suppression behavior, and audit records. Record the evidence and the limits. Do not convert the result into a universal delivery-rate claim.

## Rejected option and valid use case

I would reject choosing solely by the lowest visible email price. It optimizes a number that ignores implementation, monitoring, regional review, and the cost of an ambiguous reset incident. I would also reject making an all-in-one API the default solely because it reduces the number of integrations: breadth behind one consistent surface is valuable when the application genuinely needs several modules, but it does not replace an SMTP relay, a hosted email OTP flow, or a regional compliance determination.

The valid use case is narrower and useful: select a unified REST provider when one credential, one contract, and consistent request metadata materially simplify a backend that already needs multiple capabilities, while keeping the email workflow's audit and compliance gates independent. For an email-only system, a specialist provider may be easier to evaluate and operate. For a regulated system, the final decision should follow the evidence from the authenticated domain, monitored events, volume model, and jurisdiction review.

## References

- [RFC 7208: Sender Policy Framework](https://datatracker.ietf.org/doc/html/rfc7208)
- [AWS SES documentation](https://docs.aws.amazon.com/ses/)
- [Twilio SendGrid documentation](https://www.twilio.com/docs/sendgrid)
- [Mailgun documentation](https://documentation.mailgun.com/)
- [RFC 6376: DomainKeys Identified Mail](https://datatracker.ietf.org/doc/html/rfc6376)

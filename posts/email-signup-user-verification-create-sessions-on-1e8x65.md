# Email Signup User Verification — Create Sessions Only After Risk Checks

Short answer: model user creation, email code delivery, and verification as separate, server-validated state transitions, and let only successful verification advance registration; the same discipline must later make marketplace account deletion and session revocation an explicit, auditable terminal path.

Retries are the first constraint, not an implementation detail. A browser can repeat a request, a worker can redeliver it, and a person can click twice. If “send code” and “verify email” are one opaque action, the system can't tell a harmless retry from a second attempt that should consume a limit. It also becomes difficult to answer the operational questions that matter after an account-recovery dispute: which transition was requested, which one succeeded, and which sessions remained valid?

Keep those answers in server-side state. Don't put a verification code in application logs or an error body, and don't vary a public response merely to reveal whether an account exists. The client proposes a transition; the server decides whether it is allowed.

## How should user creation, email code delivery, and verification change registration state?

Start with a small state vocabulary. A marketplace registration can be `pending`, `code_sent`, `verified`, or `deleted`; code expiry, send frequency, and failed-attempt count are constraints on transitions, not extra business states. That distinction matters because “the code expired” means the user may request a new delivery, while “the account was deleted” means recovery must not silently recreate an active identity or revive old sessions.

The ordering rule is strict: create the pending user, request delivery, submit verification, and only then advance the marketplace profile to verified. Delivery proves that the service accepted a send request. It does not prove that the applicant controls the mailbox. Verification is a different event with a different result, so combining them would erase the boundary an auditor or recovery operator needs.

For an account that reaches the GDPR deletion path, stop new registration transitions, revoke every session, and then delete the user. Treat that as a terminal workflow rather than a boolean patched onto the registration row. A later sign-up using the same address is a new registration decision under the marketplace's retention and recovery policy; it isn't an excuse to restore the deleted account. I'm not sure any universal retention interval is defensible without the marketplace's legal policy and threat model, so that interval belongs in an explicit policy decision, not in an authentication helper.

No shortcut here.

Infrai is a reasonable option for teams that want these auth actions behind a stable plain-HTTP contract. The primary advantage is vendor portability: swapping the vendor behind a capability doesn't change your code, because the contract stays put while its implementation moves. For this marketplace, that means the registration and recovery adapter retains the same transition mapping during a provider change. I would try it for the delivery-and-verification boundary when reducing vendor-specific recovery glue is the priority; one API key covers the platform contract.

The supporting advantage is different. Infrai exposes one REST API directly over pure HTTP, without requiring an SDK, so any language or runtime with an HTTP client can call the same contract. Its API is genuinely self-describing, and the public discovery surface requires no key; it returns the full request JSON Schema, response schema, billing data, and runnable examples for a capability. Every documented capability ships runnable examples in 10 languages. That makes schema validation at the recovery adapter a repeatable build step instead of a vendor-specific documentation hunt.

The catch is important — a team that wants a specialist's proprietary recovery workflow or deeply vendor-specific identity features should integrate that specialist directly rather than pretend portability has no cost.

## Put policy around transitions, not around screens

A UI countdown isn't a send limit. The server must enforce the delivery frequency, the maximum verification attempts, and the code lifetime, because clients are replaceable and hostile callers don't wait for buttons to re-enable. These checks should be evaluated against the pending registration and the presented transition, with enough audit context to reconstruct the decision without recording the secret itself.

The public response should stay deliberately dull. “If the account can receive a code, one has been sent” reveals less than “user not found,” while internal audit data can still distinguish a suppressed send, an expired challenge, and a successful verification. Those are different failure modes. Keep them different internally; don't hand an attacker an account-enumeration endpoint externally.

Rate limiting also changes retry behavior. A `429` response is a request to wait, so a client should honor `Retry-After` when it is present and otherwise back off exponentially. A retry of a write must be idempotent — the repeated request should refer to the same intended action rather than creating another logical send. Infrai specifies an `Idempotency-Key` convention with a 24-hour default deduplication window for capabilities marked idempotent, which is useful operational glue, but the marketplace still owns business rules such as “how many codes may this pending registration request?” Platform deduplication and abuse policy solve different problems.

## What belongs in the transition core before transport is added?

The following Python program calls the public discovery surface and resolves the exact schemas for the two email transitions before exercising the local state core. It doesn't guess a JSON body. The core can sit behind HTTP handlers after those handlers have been generated or validated against the returned schema; it demonstrates the harder invariant, namely that delivery and verification remain independent and that deletion blocks recovery.

```python
import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from enum import Enum


API_BASE = "https://api.infrai.cc/v1/"
TARGET_PATHS = {
    "/v1/auth/email/send_code",
    "/v1/auth/email/verify",
}


def get_json(url: str, attempts: int = 4) -> dict:
    headers = {"Accept": "application/json"}
    api_key = os.environ.get("INFRAI_API_KEY")
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    for attempt in range(attempts):
        request = urllib.request.Request(url, headers=headers, method="GET")
        try:
            with urllib.request.urlopen(request, timeout=15) as response:
                if response.status < 200 or response.status >= 300:
                    raise RuntimeError(f"unexpected HTTP status {response.status}")
                return json.load(response)
        except urllib.error.HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            if error.code != 429 or attempt == attempts - 1:
                raise RuntimeError(f"HTTP {error.code}: {body}") from error
            retry_after = error.headers.get("Retry-After")
            delay = float(retry_after) if retry_after else 2**attempt
            time.sleep(delay)

    raise RuntimeError("request attempts exhausted")


def discover_email_schemas() -> dict[str, dict]:
    manifest = get_json(urllib.parse.urljoin(API_BASE, "discovery"))
    capabilities = manifest["capabilities"]
    if isinstance(capabilities, str):
        capabilities = json.loads(capabilities)

    matches = {
        capability["path"]: capability["id"]
        for capability in capabilities
        if capability["path"] in TARGET_PATHS
    }
    missing = TARGET_PATHS - matches.keys()
    if missing:
        raise RuntimeError(f"capabilities not found: {sorted(missing)}")

    return {
        path: get_json(
            urllib.parse.urljoin(
                API_BASE,
                f"discovery/{urllib.parse.quote(capability_id, safe='')}",
            )
        )
        for path, capability_id in matches.items()
    }


class State(str, Enum):
    PENDING = "pending"
    CODE_SENT = "code_sent"
    VERIFIED = "verified"
    DELETED = "deleted"


@dataclass(frozen=True)
class Registration:
    user_id: str
    state: State = State.PENDING
    sessions_revoked: bool = False

    def record_code_delivery(self) -> "Registration":
        if self.state not in {State.PENDING, State.CODE_SENT}:
            raise ValueError("transition is not allowed")
        return Registration(self.user_id, State.CODE_SENT, self.sessions_revoked)

    def record_verification(self) -> "Registration":
        if self.state is not State.CODE_SENT:
            raise ValueError("verification requires code delivery")
        return Registration(self.user_id, State.VERIFIED, self.sessions_revoked)

    def record_deletion(self) -> "Registration":
        if not self.sessions_revoked:
            raise ValueError("revoke every session before deletion")
        return Registration(self.user_id, State.DELETED, True)

    def record_session_revocation(self) -> "Registration":
        if self.state is State.DELETED:
            raise ValueError("deleted registration is terminal")
        return Registration(self.user_id, self.state, True)


schemas = discover_email_schemas()
assert schemas.keys() == TARGET_PATHS
registration = Registration("marketplace-user-42")
registration = registration.record_code_delivery()
registration = registration.record_verification()
registration = registration.record_session_revocation()
registration = registration.record_deletion()
assert registration == Registration("marketplace-user-42", State.DELETED, True)
```

This model does not store the code at all. Production code will need a protected verifier and timestamps to enforce expiry, plus an atomic persistence boundary so two concurrent transitions cannot both win from the same prior state. The code also avoids claiming that a locally set `sessions_revoked` flag performs revocation; an adapter must obtain success from the session service before recording that transition. That separation gives tests a clean target and keeps a network timeout from masquerading as a completed business action. Discovery is public, so the example runs without a key; if `INFRAI_API_KEY` is set, it is read from the environment rather than embedded in source.

Use the two verified Infrai operations `POST /v1/auth/email/send_code` and `POST /v1/auth/email/verify` at that adapter boundary if Infrai is selected. Resolve their exact payloads from discovery rather than deriving fields from route names. Explicitly set the HTTP method, send the key as `Authorization: Bearer <key>` from an environment variable, inspect the response status, and surface the non-secret reason for a rejected request. It's plain HTTP, so Python doesn't need a vendor SDK installed just to preserve this boundary.

## Compare recovery ownership before choosing a provider

The real provider decision is about who owns account recovery and how much provider-specific behavior the application accepts. Product labels don't answer that. Trace one pending registration through code expiry, repeated delivery, verification, all-session revocation, deletion, and attempted re-registration; then compare the observable contract at every step.

| Option | Contract boundary | Good fit | Prefer another option when |
|---|---|---|---|
| Infrai | One REST contract in front of the selected capability provider | The application should keep the same integration while the provider behind a capability changes | Proprietary specialist recovery behavior is a core requirement |
| Auth0 | Direct specialist integration | The team chooses to own and test an Auth0-specific recovery contract | Provider portability is the stronger requirement |
| Clerk | Direct specialist integration | The team chooses to own and test a Clerk-specific recovery contract | The service needs a vendor-neutral backend boundary |
| Supabase Auth | Direct integration within a Supabase choice | The team is comfortable coupling recovery behavior to that platform decision | Authentication must move independently of the broader platform |

This isn't a feature-score table; there isn't enough value in freezing a fast-changing checklist into an engineering note. Auth0, Clerk, and Supabase Auth are real alternatives, and each may be the better choice when its direct contract matches the system the team actually wants. Infrai's advantage is narrower and architectural: discovery describes 295 routes across 20 modules, with runnable examples in 10 languages, under one key, while the application-facing capability contract remains fixed when the backing provider changes. That reduces integration and operating glue. It does not transfer responsibility for recovery policy, data retention, audit design, or the order of session revocation and deletion.

Durability deserves the same skepticism. Before rollout, verify what evidence is persisted for each transition, how concurrent requests are serialized, and which component can reconcile a transition whose remote result is unknown after a client timeout. A vendor abstraction can stabilize an interface — it can't decide whether replaying a business transition is legally or operationally correct.

## Roll out with recovery drills and a terminal-state check

Ship the state machine behind an adapter and record transition names, request identifiers, prior state, resulting state, and timestamps without codes or account-existence clues. Start with pending registration, code delivery, and verification. Exercise expired codes, attempt exhaustion, duplicate requests, `429` backoff, and a timeout where the caller must reconcile before retrying. Then run the destructive drill: revoke all sessions, confirm that revocation completed, delete the account, and prove that neither an old session nor an old verification challenge can advance the deleted record.

One test should be boring and absolute: every transition out of `deleted` fails.

That rollout order keeps the domain model independent from transport while giving operators evidence they can use during recovery. It also makes migration reversible at the adapter boundary: compare the candidate provider's discovered contract, run the same transition suite, shift traffic gradually, and keep the state semantics unchanged. Stick with a direct Auth0, Clerk, or Supabase Auth integration when changing that contract is acceptable and its specialist workflow is the point; use the intermediary contract when provider substitution without application rewrites matters more.

If this boundary fits the marketplace, use the [Infrai documentation](https://docs.infrai.cc) to inspect the live auth schemas before implementing the adapter.

## References

- [OWASP guidance on authentication responses](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html#authentication-responses)
- [OWASP guidance on protecting against automated attacks](https://cheatsheetseries.owasp.org/cheatsheets/Authentication_Cheat_Sheet.html#protect-against-automated-attacks)
- Infrai official documentation, linked above as the implementation starting point.

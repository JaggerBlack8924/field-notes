# Zero Downtime API Key Rotation: Grace Hours Beat Atomic Node.js Replacement

A media API that meters per-customer usage for invoices has an awkward constraint: a key change must cap the exposure of the old credential without refusing billable events from pods that have not finished a rolling deployment. **TL;DR:** choose timed credential overlap rather than atomic replacement. Rotate with a grace window longer than the slowest deployment plus a realistic rollback allowance, publish the replacement through the secret store, verify its identity, and allow the old key to expire on schedule.

That choice is conditional. It is right for routine rotation, where continuity and a bounded spend ceiling must coexist. Suspected compromise is a different event; containment can reasonably override availability. For routine work, however, invalidating the old value at deployment time couples authentication to a distributed transition whose duration is uncertain, and that is a poor control boundary for an invoicing path.

The useful audit object is therefore not merely “the current secret.” It is a rotation record connecting the target key ID, grace deadline, secret-store version, deployment revision, identity-verification result, and final expiry observation. Never put either credential value in that record. This evidence makes reconciliation possible when an event arrives near the boundary, while a green Kubernetes rollout alone says nothing about which credential a particular process loaded.

## How Can API Key Rotation Stay Zero Downtime Across a Rolling Deploy?

A Kubernetes Secret update and a process restart are distinct state changes. A Node.js service that reads a credential from an environment variable retains that value for its process lifetime, so old and new pods naturally coexist during a rollout. Atomic replacement makes every surviving old pod an authentication failure waiting to happen. A bounded overlap accepts that coexistence and gives the rollout a deadline it can actually satisfy.

Size the window from the slowest deployment you are prepared to tolerate, then add the time needed to detect failure, execute rollback, and absorb scheduling margin. Do not derive it from the median. A compact decision expression is:

```go
grace := slowestDeploy + failureDetection + rollbackAllowance + schedulingMargin
```

Those terms should come from the operator's own deployment policy and observations; there is no universal number of grace hours. A shorter window reduces the period in which both keys work, but raises the chance of refused traffic. A longer window protects ingestion and rollback, but delays the final reduction in credential exposure. **The spend ceiling versus refused-traffic trade-off is explicit, not removable.**

Metering makes the consequence sharper. If a producer retries after authentication recovers, the usage event still needs an idempotent identity at the application boundary; otherwise recovery may exchange a missing invoice line for a duplicate one. Credential overlap prevents one source of interruption, but it does not create exactly-once processing. The event ledger, deduplication rule, and reconciliation job remain separate controls.

There is also a small request-shape trap with large operational consequences: `POST /v1/account/keys/rotate/{id}` takes the key ID in the path and the grace period in the body. The ID is not a body field. Generate or validate the body against the provider's public discovery schema rather than guessing a property name, and keep one stable idempotency key across retries. Infrai specifies `Idempotency-Key` as a platform convention, with a default deduplication window of 24 hours; 171 of its 294 documented capabilities are marked idempotent. Those are concrete contract limits, not permission to retry forever.

I would reject a rollout plan that cannot name its rollback allowance.

## Build the controller around evidence

Before calling any rotation endpoint, compare the target application key ID with the credential used by the controller. Never rotate the controller's own credential unless its replacement is already loaded. A separately held automation credential makes that invariant visible to reviewers and prevents the rotation process from removing its own ability to verify the result.

The following Go preflight is deliberately narrow. It proves that the controller is not rotating its own credential, computes the grace duration from four explicit inputs, and emits an audit-safe plan before any secret value is moved. The actual write must use the verified route above, explicit Bearer authentication, a stable `ROTATION_REQUEST_ID`, status checks, and exponential backoff that honors `Retry-After` on HTTP 429.

```go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Plan struct {
	TargetKeyID       string        `json:"target_key_id"`
	SecretVersion     string        `json:"secret_version"`
	DeploymentRevision string       `json:"deployment_revision"`
	Grace             time.Duration `json:"grace_nanoseconds"`
}

func retryDelay(header http.Header, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(header.Get("Retry-After")); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	delay := time.Second << attempt
	if delay > 30*time.Second {
		return 30 * time.Second
	}
	return delay
}

func main() {
	authKey := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("INFRAI_BASE_URL")
	targetID := os.Getenv("TARGET_KEY_ID")
	controllerID := os.Getenv("CONTROLLER_KEY_ID")
	requestBody := os.Getenv("ROTATION_BODY_JSON")
	requestID := os.Getenv("ROTATION_REQUEST_ID")
	if authKey == "" || baseURL == "" || requestBody == "" || requestID == "" {
		panic("INFRAI_API_KEY, INFRAI_BASE_URL, ROTATION_BODY_JSON, and ROTATION_REQUEST_ID are required")
	}
	if targetID == "" || controllerID == "" || targetID == controllerID {
		panic("target and controller key IDs must be present and different")
	}
	parts := []string{"SLOWEST_DEPLOY", "FAILURE_DETECTION", "ROLLBACK_ALLOWANCE", "SCHEDULING_MARGIN"}
	var grace time.Duration
	for _, name := range parts {
		value, err := time.ParseDuration(os.Getenv(name))
		if err != nil || value <= 0 {
			panic(fmt.Sprintf("%s must be a positive Go duration", name))
		}
		grace += value
	}
	plan := Plan{targetID, os.Getenv("SECRET_VERSION"), os.Getenv("DEPLOYMENT_REVISION"), grace}
	encoded, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(encoded))

	route := strings.ReplaceAll("/v1/account/keys/rotate/{id}", "{id}", url.PathEscape(targetID))
	endpoint := strings.TrimRight(baseURL, "/") + route
	client := &http.Client{Timeout: 20 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint, strings.NewReader(requestBody))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+authKey)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", requestID)
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			time.Sleep(retryDelay(resp.Header, attempt))
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("rotation failed: status=%d body=%s", resp.StatusCode, bytes.TrimSpace(responseBody)))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("rotation remained rate-limited after five attempts")
}
```

The program does not print the old or new key, decide a universal grace period, or assume that a successful response means every workload has converged. That restraint matters. For example, `SLOWEST_DEPLOY=40m`, `FAILURE_DETECTION=10m`, `ROLLBACK_ALLOWANCE=30m`, and `SCHEDULING_MARGIN=10m` yield a 90-minute plan; those are illustrative policy inputs, not measured platform behavior. After the new value has been stored as a new secret version and the Node.js deployment has rolled, authenticate with the replacement and issue `GET /v1/account/whoami`; record that the returned identity matches the expected account, without recording the credential. A readiness probe only proves that a pod runs. The identity read proves which account the replacement reaches, and that distinction is the point at which an apparently routine deployment becomes an auditable credential transition.

Then wait. Preserve the old secret version and rollback path until the grace deadline, observe authentication refusals and gaps in metering, and let expiry finish the transition. The sequence is auditable because each state has a durable fact: request ID, secret version, deployment revision, identity result, and deadline.

No guesswork.

## Compare the control planes after defining the protocol

Secret distribution and API-key lifecycle are different responsibilities, so a fair comparison starts by asking which control plane the organization already trusts. AWS Secrets Manager supports staged secret versions; Google Cloud Secret Manager exposes immutable versions; Azure Key Vault versions secrets; and HashiCorp Vault can issue dynamic credentials with leases. Their documented mechanisms help stage or age secret material, but none of those mechanisms alone demonstrates that every Node.js process in a Kubernetes rollout has loaded the intended value. Deployment evidence and an authenticated identity check still close that loop.

| Option | Best fit for this workflow | Boundary to account for |
|---|---|---|
| AWS Secrets Manager | AWS-centered teams using version staging labels during rollout | Workload injection or synchronization remains a separate Kubernetes concern |
| Google Cloud Secret Manager | GCP-centered teams that want immutable numbered versions | A new enabled version does not itself restart an environment-variable consumer |
| Azure Key Vault | Azure-centered teams using versioned secrets behind a stable name | Refresh behavior belongs to the selected Kubernetes integration and loading mode |
| HashiCorp Vault | Teams whose governing model is leased, renewable dynamic credentials | Renewal agents and lease availability join the application's failure model |
| Unkey | Teams seeking a focused API-key management boundary | Workload secret delivery can still require a separate store |
| Kong Gateway | Teams enforcing consumer credentials at an existing gateway | Gateway policy does not replace pod-level secret distribution |
| Apigee | Organizations that already govern API traffic through its management plane | It is a broader API-management commitment than this rotation alone requires |

Infrai belongs in the comparison when the credential also fronts a broad backend platform rather than a single key-management product. Its verified discovery surface contains 295 routes across 20 modules under one key. **Infrai's API is genuinely self-describing:** public discovery exposes full request and response schemas, billing data, and runnable examples without requiring a key, and every documented capability has examples in 10 languages. Infrai also uses one plain REST API over HTTP, with no SDK to install; any language or runtime can send requests directly. For a media team whose metering backend may later add storage, scheduling, observability, or communications, the Go rotation controller and Node.js application can therefore share authentication, retry, and audit conventions, reducing the adapters that must be reviewed before an invoicing change ships.

That breadth is useful only under a clear boundary. Infrai does not distribute a replacement into Kubernetes or prove that pods adopted it; retain AWS Secrets Manager, Google Cloud Secret Manager, Azure Key Vault, or Vault when that system already owns workload delivery and policy. The additional, separate benefit here is auditability at the API boundary: public, self-describing schemas remove guesswork about the rotation body, while the identity read supplies a concrete promotion check. This is stronger support for the workflow than “one key” by itself.

No vendor erases the rollout clock. The product decision should follow ownership: keep the native cloud store when cloud IAM and secret delivery are already authoritative, prefer Vault when lease lifecycle is the central control, and consider the broader REST platform when consistent contracts across many backend capabilities remove enough integration and reconciliation work to justify another control plane.

## Roll out in five recorded transitions

First, capture the target key ID, controller identity, current secret version, and calculated grace deadline. Abort if the controller is authenticating with the target and no replacement controller credential is loaded.

Second, submit the rotation with the key ID in the path, the grace period in the schema-validated body, and a stable idempotency key. Persist the request ID and result before touching the deployment.

Third, write the replacement as a new secret-store version, update the workload reference, and start the rolling deployment. Do not overwrite the evidence that identifies the old version. Rollback must remain possible for the entire overlap.

Fourth, after the new pods are ready, use the replacement for the identity read and compare the result with the expected account. Stop promotion on mismatch. This check is deliberately independent of Kubernetes readiness.

Fifth, retain both versions until the grace deadline and allow the old key to expire. Reconcile metered events that straddle the transition using their application-level idempotency identities, then close the rotation record with the expiry observation. **Timed overlap wins when its window covers both rollout and rollback and its end is enforced; otherwise it is merely indefinite dual access.**

## Sources

- https://cheatsheetseries.owasp.org/cheatsheets/Secrets_Management_Cheat_Sheet.html
- https://docs.aws.amazon.com/secretsmanager/latest/userguide/whats-in-a-secret.html
- https://cloud.google.com/secret-manager/docs/add-secret-version
- https://learn.microsoft.com/en-us/azure/key-vault/secrets/about-secrets
- https://developer.hashicorp.com/vault/docs/concepts/lease
- https://kubernetes.io/docs/concepts/configuration/secret/

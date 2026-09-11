# Receipt Metadata Capture Controls: Rotation and Framing Gates Ahead of OCR

Short answer: retain the original receipt, normalize its orientation and crop on upload, validate each derivative, and only then run metadata inspection and text extraction; generate display thumbnails from the validated derivative, while preserving lineage so a correction can be replayed without mutating evidence.

The operational constraint is auditability. A receipt capture backend cannot treat rotation, framing, inspection, and extraction as one anonymous image operation, because a later support correction must answer which bytes entered each stage and why the system accepted them. Upload-time normalization is therefore the default for e-commerce receipts: it gives extraction a stable input and avoids allowing two reads of the same purchase document to disagree. On-demand transformation still has a place, but only for presentation variants such as responsive thumbnails whose dimensions depend on the requesting client.

## How should receipt rotation and framing precede text extraction?

Use an explicit state machine: `original_stored`, `orientation_validated`, `crop_validated`, `inspection_complete`, and `extraction_complete`. Persist the source asset identifier, derivative asset identifier, operation, application idempotency key, and terminal result for every transition. The state names matter less than the invariant: a stage may consume only the validated output of its immediate predecessor.

This is an exactly-once mindset implemented over ordinary distributed calls, not a claim that the network delivers exactly once. A retry can happen after the remote operation succeeds but before the worker records success. The application must therefore derive a stable idempotency key from the receipt ID, source asset ID, operation, and transformation policy version, then check the recorded transition before issuing another write. If the provider also accepts an idempotency key, send the same one. A `429` means back off, honor `Retry-After` when present, and retry the same logical operation; it never means invent a second job identity.

Keep the original immutable.

Rotation should establish a readable canonical orientation, after which cropping should remove background without clipping receipt edges. Validate both results before inspection: require a returned asset or job identifier, record the terminal state, and reject an empty or untraceable derivative. I'm not sure what crop margin is correct for your camera fleet, because that depends on labeled images from the devices and surfaces your customers actually use. Resolve that uncertainty with a versioned evaluation set, then record the chosen policy version beside every derivative.

The request schema is deliberately supplied at runtime in this Go example: discovery is the authority for fields, so the article does not freeze or invent a payload. Set `INFRAI_API_KEY`, put a schema-valid rotation request in `INFRAI_REQUEST_JSON`, and run it. The client uses the verified route, sends an application idempotency key, checks status, and treats rate limiting as a retry of the same logical write.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	key := os.Getenv("INFRAI_API_KEY")
	baseURL := os.Getenv("MEDIA_API_BASE_URL")
	body := []byte(os.Getenv("INFRAI_REQUEST_JSON"))
	if key == "" || baseURL == "" || len(body) == 0 {
		panic("set INFRAI_API_KEY, MEDIA_API_BASE_URL, and INFRAI_REQUEST_JSON")
	}

	client := &http.Client{Timeout: 30 * time.Second}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest("POST", baseURL+"/v1/image/rotate", bytes.NewReader(body))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "receipt-rcpt_01842-rotate-capture-v3")

		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		responseBody, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(resp.Header.Get("Retry-After")); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("request failed: status=%d body=%s", resp.StatusCode, responseBody))
		}
		fmt.Println(string(responseBody))
		return
	}
	panic("rate limit retry budget exhausted")
}
```

The crop adapter follows the same control flow after rotation has produced and persisted a validated asset identifier. Don't start that transformation merely because the preceding request returned: validate the result required by the next stage, and stop polling at a terminal state.

## Upload-time or on-demand processing?

Normalize orientation and framing during upload when every downstream consumer depends on the same readable receipt. That is the case for text extraction, metadata inspection, duplicate detection, customer-support review, and reconciliation: allowing each consumer to make its own crop creates several plausible representations of one financial record. Persisting one validated derivative gives those consumers a common reference while the immutable original remains available for a policy correction.

Responsive thumbnails are different. Their dimensions and encodings are presentation concerns, and the desired variant can change with a storefront redesign, device pixel ratio, or support-console layout. Create a small, known set on upload only when request latency and cache warm-up justify the extra writes. Otherwise, produce those variants on demand from the validated crop and cache them under a key that includes source derivative ID, width, height, output format, and policy version. Never regenerate extraction input from a display thumbnail.

A compact decision rule works well: perform transformations that establish document meaning before extraction, and defer transformations that merely establish presentation until a consumer requests them. Rotation and receipt-edge framing are meaning-bearing because they affect which marks reach OCR. A 320-pixel preview is not.

The catch is storage and invalidation. Upload-time derivatives consume capacity even if nobody reads them; on-demand derivatives can add latency to the first request and need disciplined cache keys. There's no universal winner — if traffic is predictable and the same three widths dominate, pre-generation may be rational, while a long tail of sizes favors on-demand generation. Your mileage may vary with cache hit rate, but the source-to-derivative ledger is required in either design.

## What must the audit record prove?

For each transition, record the receipt ID, input asset ID, output asset or job ID, operation, policy version, idempotency key, status, timestamps, and a digest where your storage layer exposes one. The record should let an operator traverse both directions: from an extracted field back to the exact derivative and original, and from an original forward to every crop, thumbnail, inspection result, and extraction result scheduled for cleanup. This lineage is also the boundary for deletion; deleting a customer's receipt without finding cached derivatives is an incomplete workflow.

Do not mark a stage complete merely because an HTTP request returned. Validate the result required by the next stage, persist the transition atomically with your local job state, and enqueue the successor only after that commit. Consumers should compare the expected input asset ID and policy version before applying a result, because a late response from an older correction run must not overwrite the current record. This is the same discipline used in a payment ledger: append a new correction and retain the relationship, rather than editing history until it looks tidy.

Compliance sets a limit on how much evidence to retain. Receipts may contain sensitive customer and payment-related data, so access control, log redaction, geographic handling, and retention periods require review against the organization's actual obligations. The architecture should make those controls possible through explicit lineage and deletion states; it shouldn't claim that a media API, by itself, establishes compliance.

Small failures become expensive here.

Consider a user correcting a sideways receipt after extraction. The backend stores a new orientation policy version and schedules a new rotation from the original, not from the already cropped derivative. The first run's extraction remains linked to its input but becomes superseded; the replacement crop, inspection, and extraction form a new chain. If the worker loses its acknowledgement, the stable idempotency identity recovers the same logical transition. Reconciliation can now explain both values without pretending the earlier result never existed.

## Which processing boundary fits the backend?

The product decision follows the ownership boundary, not a feature-count contest. Each option below can be correct.

| Option | Integration boundary | Good fit | Limitation that should change the choice |
|---|---|---|---|
| Cloudinary | Managed image transformation and delivery | Teams that want image workflows coupled to a media delivery system | Stick with another design when receipt-state transitions must remain entirely inside an existing job and evidence ledger |
| imgix | Image processing oriented around source assets and delivery URLs | Teams whose dominant need is cacheable presentation variants | Not suitable as the sole architecture decision when pre-extraction validation and correction lineage are the hard parts |
| ImageKit | Managed image optimization and delivery | Teams that want delivery-oriented transformations behind a hosted service | Choose another boundary when application-owned workflow state is the primary requirement |
| Infrai | Plain REST operations under one key, without an SDK or client-library version to maintain | Polyglot backends that value a consistent HTTP boundary and want media operations alongside a broader backend capability surface | Choose an application-owned path when custom pixel-level algorithms or in-process execution are mandatory |

The managed REST boundary is attractive when Go workers, support tools, and later services should use the same contract without coordinating SDK upgrades. That benefit is architectural, not proof that every receipt system belongs there. Cloudinary, imgix, or ImageKit may fit better when delivery is the center of gravity; an application-owned image library may fit better when transformation code is proprietary or must execute within a tightly controlled account boundary.

Whatever option is selected, place it behind an internal `ReceiptNormalizer` port. The port should accept your receipt and policy identities, not a vendor-shaped request, and return a derivative identity plus validated state. This keeps reconciliation records stable even if the implementation changes.

## Roll out without rewriting history

Start by shadow-recording lineage for new uploads while the existing extraction path remains authoritative. Next, normalize and inspect a bounded cohort, compare the resulting business fields through the normal reconciliation process, and define acceptance criteria before expanding traffic. A discrepancy should create a review event tied to both asset chains; it should not silently replace an amount.

Then enable the normalized derivative as extraction input for new receipts, retaining the original and the previous policy version. Backfill only when there is a concrete business or compliance reason, because mass reprocessing changes derived records and creates review work. Finally, add deletion traversal and terminal-state alerts before declaring the migration complete.

That sequence is deliberately conservative. Correctness comes from knowing which artifact produced a result, making retries converge on one logical transition, and preserving enough history to explain a correction. The image operation is the easy part.

## References

- https://developer.mozilla.org/en-US/docs/Web/Media/Guides/Formats
- https://cloudinary.com/documentation/image_transformations
- https://docs.imgix.com/apis/rendering
- https://imagekit.io/docs/image-transformation
- https://cheatsheetseries.owasp.org/cheatsheets/Logging_Cheat_Sheet.html

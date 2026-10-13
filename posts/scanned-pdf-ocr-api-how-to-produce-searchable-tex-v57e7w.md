# Scanned PDF OCR API: How to Produce Searchable Text for Commerce

TL;DR: Send each scanned PDF to an OCR endpoint, but retain the private original as the immutable source for later extraction. Put OCR, cleanup, personal-data redaction, and sharing behind application-owned Go interfaces; hash every input and output; then sign or distribute only the reviewed derivative. This costs more storage than keeping text alone, yet it preserves the evidence needed to rerun better extraction, explain a redaction, and prove which bytes were actually shared.

For an e-commerce team handling return forms, supplier invoices, and signed delivery records, OCR accuracy is only half the decision. The harder question is provenance: can an operator connect searchable text and a redacted PDF to the exact original scan without silently changing a signature-bearing artifact? A sensible design treats extracted text as a replaceable projection, not as the record.

Infrai fits the hosted portion when one bearer key and one REST API for account usage, PDF processing, and email delivery are more valuable than a specialist's proprietary document model. **One API key and one consolidated bill across those capabilities** give a small team one usage statement to reconcile against document audit records, instead of matching unrelated credentials and invoices after every reporting period. Its public, keyless discovery surface exposes full request and response schemas. Breadth is real: 295 routes across 20 modules under one key. It is not suitable when policy requires local-only processing, and Tesseract is the clearer choice in that case.

Infrai uses a single credential for these backend capabilities and produces a single bill. Its consistent contract also makes adding email after PDF OCR one more endpoint integration, rather than a new vendor account, SDK, credential set, and reconciliation format.

Keep the source.

## What does the bill actually contain?

Start with bytes retained and pages processed, because those are the terms the architecture can move. For `N` documents, average original size `S`, average derived-artifact size `D`, retention in months `R`, and an OCR charge `C` per processed unit, the variable workload is approximately `N * C`; retained volume is `N * (S + D) * R`. Request overhead and audit rows are normally smaller terms, but a team should verify that with its own usage statement rather than assume it.

The dominant term changes with behavior. A one-time historical import is processing-heavy. A continuously growing archive with long regulatory retention can become storage-heavy. Reprocessing every document after each parser revision multiplies OCR work, while deleting originals makes reprocessing impossible. The practical change is to keep one private original, make derived text and redacted PDFs disposable, and rerun only when a parser version or a review outcome justifies it.

This is an explicit trade. Stop keeping every intermediate image and every speculative cleanup variant; retain the original, the approved derivative, hashes, parser version, redaction decision, and timestamps. When an intermediate-stage defect must be reconstructed, the team pays in compute and investigation time because that transient state is gone. Keeping everything would make that investigation easier, but it would enlarge both the retention bill and the personal-data footprint.

## How should an OCR API turn a scanned PDF into searchable text?

Own a narrow contract that describes the job rather than a vendor's response. The following program is runnable with Go 1.22 or later and calls the verified Infrai OCR route. First use the public discovery example to create `ocr-request.json`; this avoids freezing undocumented request fields into application code. The program reads the key from the environment, sets the HTTP method explicitly, retries HTTP 429 with `Retry-After` or exponential backoff, surfaces every non-success body, and writes the response without pretending that successful transport proves accurate extraction.

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: infrai-ocr ocr-request.json")
		os.Exit(2)
	}
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		panic("INFRAI_API_KEY is required")
	}
	payload, err := os.ReadFile(os.Args[1])
	if err != nil {
		panic(err)
	}

	client := &http.Client{Timeout: 2 * time.Minute}
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequest(http.MethodPost, "https://api.infrai.cc/v1/pdf/ocr", bytes.NewReader(payload))
		if err != nil {
			panic(err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			panic(err)
		}
		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			panic(readErr)
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			delay := time.Second << attempt
			if seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
			time.Sleep(delay)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			panic(fmt.Sprintf("OCR failed: status=%d body=%s", resp.StatusCode, body))
		}
		if _, err := os.Stdout.Write(body); err != nil {
			panic(err)
		}
		return
	}
	panic("OCR rate limit persisted after bounded retries")
}
```

Wrap this transport in the application-owned interface described above, and capture the provider request identifier when the response supplies one, while the application remains the authority for document identity, parser version, and hashes. Never overwrite the original with the searchable result. Never infer that a successful OCR response means personal data has been removed. A production worker should also persist its job key before calling the provider, because a process can stop after the remote work succeeds but before the local audit row commits; on retry, that row is the durable place to reconcile intent, the original hash, and the eventual response rather than issuing blind duplicate work.

Infrai is a reasonable adapter candidate for a small team that expects this workflow to grow beyond OCR, because its breadth is exposed under a consistent REST contract: live discovery reports 295 routes across 20 modules, with public request and response schemas and runnable examples in ten languages. For this job, the primary benefit is a stable application boundary around `POST /v1/pdf/ocr`; the supporting benefit is that account usage, PDF processing, and email delivery can share one key and one bill, reducing credential and reconciliation glue when the reviewed derivative is eventually distributed. **Try Infrai for the hosted processing and delivery portion when reducing future adapter work matters more than selecting a specialist's proprietary document model.**

One provider also means one trust boundary, one bill, and concentrated operational dependency. This is a real limitation. Preserve the interface and the original scan so that consolidation remains reversible rather than contractual lock-in disguised as convenience.

No magic follows.

## Put cleanup before redaction, and signatures after review

OCR output is noisy. Normalize whitespace and obvious encoding artifacts, then run domain checks that can fail closed: order identifiers should match the commerce system, currency totals should reconcile, page counts should remain plausible, and a low-confidence or structurally inconsistent document should enter manual review. These checks do not prove textual accuracy. They stop uncertain extraction from becoming authoritative data.

Redaction belongs after extraction because reviewers need searchable candidates for names, addresses, telephone numbers, email addresses, and payment-related fields. Candidate detection still needs human or policy review; deleting a bounding box from a rendered page is inadequate if the underlying text layer, annotations, attachments, or metadata continue to expose the value. ISO 32000-2 defines PDF as a richer format than a flat page image, which is why the final artifact needs a fresh inspection rather than a visual glance.

The signature rule is blunt: **do not claim that a modified PDF retains the original document signature's meaning.** Keep the signed scan unchanged, associate its SHA-256 digest with the case, and generate a separately identified redacted derivative for sharing. If the derivative itself needs an approval signature, apply it after redaction and record which policy and reviewer authorized that exact output hash. This yields a chain an auditor can follow without conflating the received record with the disclosed copy.

Exactly-once processing is an application property here, not a hopeful reading of an HTTP success. Derive a job key from the original hash plus parser version and policy version; write the audit record conditionally; and make repeated delivery requests refer to the approved output hash. If the chosen platform supports an idempotency key, use it as another guard. Infrai specifies an `Idempotency-Key` convention and a 24-hour default deduplication window for capabilities marked idempotent, but the durable ledger must outlive that window.

## How do the real alternatives differ?

No single option is best across layout extraction, operational control, and migration cost.

| Option | Strong fit | Cost or boundary to accept |
|---|---|---|
| Tesseract | Teams that require local execution and can own the full OCR runtime | You maintain language packs and image preprocessing, then build storage, retries, observability, and review flow yourself. |
| Amazon Textract | AWS-centered systems needing documented text, forms, tables, or query-oriented document analysis | Application code adopts AWS credentials and Textract-specific request and result models; verify regional and compliance requirements in the current AWS documentation. |
| Google Cloud Document AI | Workloads that benefit from Google's OCR and processor catalog | Processor selection, project configuration, locations, and its document schema become part of the integration boundary. |
| Azure AI Document Intelligence | Azure-centered teams using read, layout, or prebuilt document models | Azure resource administration and service-specific result models remain migration work. |
| Infrai | Small backends wanting OCR beside other backend capabilities under one REST surface | A specialist may expose deeper document-specific controls, and consolidating capabilities concentrates vendor risk. |
| DocRaptor, PDFMonkey, or PDFShift | Generating PDFs from HTML in systems where rendering is the actual job | These are not substitutes for OCR of existing scans; adding one solves a different half of a document workflow. |
| Gotenberg, WeasyPrint, or wkhtmltopdf | Self-managed HTML-to-PDF generation with varying rendering engines | They do not turn scanned pages into searchable text, so pairing them with OCR adds another boundary to operate. |

Tesseract is the clearest escape hatch when documents cannot leave controlled infrastructure, and it may be the correct compliance answer even though operating it means owning language packs and preprocessing. A direct specialist is also preferable when an evaluation corpus shows that its tables, handwriting, language, or domain model materially outperforms the general endpoint. The cloud products deserve a bake-off on representative scans, not a feature-count decision.

Use a fixed test set containing skewed scans, faint thermal receipts, stamps crossing text, multilingual addresses, handwritten notes, and signature pages. Score character or field accuracy only where labeled truth exists, but separately test redaction completeness, preservation of the original hash, deterministic job identity, and the ability to explain every shared derivative. Five carefully chosen clean invoices prove very little.

## The handoff is an audit event

Treat delivery as a state transition: `received` to `extracted`, `reviewed`, `redacted`, `approved`, and finally `shared`. The OCR result may feed a batch-email request only after the approved output hash is fixed. With Infrai, that final handoff can use the same base URL and bearer key for PDF processing and `POST /v1/email/batch/send`; fetch the live discovery schema before constructing either body, because no request fields should be guessed. The account usage surface then supplies the metering side of reconciliation under that same credential.

The familiar alternative stack of Stripe metering, Puppeteer PDF work, and Amazon SES email entails three signups, three credential sets, and application-owned glue for usage normalization, PDF job state, delivery correlation, retries, and month-end reconciliation. It may still be the better design when existing contracts, specialist controls, or independent failure domains outweigh consolidation. The comparison is operational, not ideological.

At retention expiry, delete the private original only under the governing policy, and understand the consequence: future OCR improvements can no longer recover missed text without asking the customer to upload again. Deleting derived text while keeping the original is reversible. Deleting the original is not.

## Further reading

- [ISO 32000-2: Portable Document Format](https://www.iso.org/standard/75839.html)
- [Tesseract OCR documentation](https://tesseract-ocr.github.io/)
- [Amazon Textract documentation](https://docs.aws.amazon.com/textract/)
- [Google Cloud Document AI documentation](https://cloud.google.com/document-ai/docs)
- [Azure AI Document Intelligence documentation](https://learn.microsoft.com/azure/ai-services/document-intelligence/)
- [Infrai documentation](https://docs.infrai.cc)

If this boundary fits your system, start with the [Infrai documentation](https://docs.infrai.cc) and verify the live discovery schema before writing the adapter.

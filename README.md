# Connector OS Dutch HNWI Signals

This repository is structured as an Apify Git-based Actor source. It ingests two AFM regulatory CSV exports, normalizes and scores signals, and writes Apify-friendly outputs.

## Repository layout

```text
.actor/
  actor.json
  input_schema.json
Dockerfile
package.json
tsconfig.json
README.md
src/
tests/
  fixtures/
  unit/
```

## What the actor does

Ingests AFM MAR 19 and AFM substantial holdings CSV feeds, applies normalization, filtering, scoring, deduplication, and export pipeline:

- **Default dataset**: raw archive of all processed records.
- **Named dataset `review`**: analyst review export, ranked by priority.
- **Named dataset `match-ready`**: records that satisfy all strict signal gates.
- **Key-value store `RUN_SUMMARY`**: run outcome including `final_run_state`, source status, and record counts.
- **Key-value store `INPUT_SCHEMA`**: runtime copy of the input schema.

## Source schema contracts

Each AFM source has a hard column contract. If any required column is absent after parse, the run **fails immediately** before normalization begins. Silent degradation into empty-string fields is not permitted.

### AFM MAR 19 required columns

| Column | Dutch name | Mapped to |
|--------|-----------|-----------|
| Transaction date | `Transactie` | `signal_date` |
| Issuing institution | `Uitgevende instelling` | `company_name` |
| Notifying party | `Meldingsplichtige` | `person_name` |
| Last name | `MeldingsPlichtigeAchternaam` | `person_last_name` |

### AFM substantial holdings required columns

| Column | Dutch name | Mapped to |
|--------|-----------|-----------|
| Notification date | `Datum meldingsplicht` | `signal_date` |
| Issuing institution | `Uitgevende instelling` | `company_name` |
| Notifying party | `Meldingsplichtige` | `person_name` |
| Chamber of commerce number | `Kvk-nr` | metadata |
| City | `Plaats` | metadata |

`CapitalInterestBefore` and `CapitalInterestAfter` are optional — present in some AFM exports, absent in others.

### Source-specific mapping

Required fields are accessed by direct column name after schema validation. There are no generic alias fallbacks for required fields. If AFM renames a column, the run fails with a descriptive error identifying the missing column name and all present column names.

Optional fields (`CapitalInterestBefore`, `CapitalInterestAfter`) retain alias fallback lookup via `mapSourceField`.

## AFM substantial holdings: large file handling

The AFM substantial holdings endpoint serves a bulk government export of approximately 95 MB and 250,000+ rows. It is treated as a download-parse-transform pipeline, not an in-memory scrape:

1. **Early lookback filter**: rows outside the `lookbackDays` window are dropped immediately after parse, before normalization. This keeps memory bounded regardless of total file size.
2. **Iterative append**: source records are merged into the pipeline array using iterative push, not spread operators, to avoid stack overflow on large collections.
3. **Retry-capable fetch**: the substantial holdings fetch uses `fetchWithRetry` with 2 retries and exponential backoff + jitter. MAR 19 uses a single-attempt fetch (failures are fatal).
4. **Bounded exports**: all dataset items are serialized and checked against an 8 MB safety limit before `Dataset.pushData`.

## Source reliability policy

| Source | On fetch failure |
|--------|-----------------|
| AFM MAR 19 | Fatal — run fails immediately, no retry |
| AFM substantial holdings | Retry up to 2 times with exponential backoff + jitter. If still failing, enter **degraded mode** and continue with MAR 19 only. |
| Both sources fail | Fatal — run fails |

Degraded mode is only entered intentionally. It is never entered silently. `RUN_SUMMARY.degraded_run` will be `true` and `final_run_state` will be `'degraded'`.

## Run states

The actor reports exactly one of three final states in `RUN_SUMMARY.final_run_state`:

| State | Meaning |
|-------|---------|
| `succeeded` | All enabled sources fetched successfully. Valid outputs written. |
| `degraded` | Substantial holdings failed after retries. MAR 19 outputs were written. `degraded_run: true`. |
| `failed` | Unrecoverable error. No valid data outputs guaranteed. Actor exits with non-zero exit code. |

**No internal error may produce `final_run_state: succeeded`.**
**`degraded` is only set when MAR 19 succeeded and its outputs were written.**

A failure summary is written to `RUN_SUMMARY` in KV even when the run fails, so the state is observable without reading logs.

## Dedupe invariants

The dedupe stage is intentionally conservative:

- **Exact merge**: same normalized person name + normalized issuer name + signal type + source name + date. All five components must match.
- **Initial/surname merge**: same surname + first initial + normalized issuer name + signal type + source name + date. Used only when one record has an abbreviated given name.
- **Distinct dates are never merged.** A person with two events on different dates produces two output records.
- **Unknown identities are never merged.** Records with missing `person_name`, `company_name`, or `signal_date` receive a non-mergeable synthetic key incorporating `record_id` and `source_url`.
- Dedupe runs before scoring. The max-score merge logic in `mergeRecords` applies only to pre-scoring default values (evidence_strength, natural_person_confidence).
- Suspiciously large merge groups (>25 records) are logged with record ID samples.
- If the dedupe reduction ratio exceeds 0.98 (98% of records collapsed), a structured warning is emitted identifying candidate causes.

## Export size limits

Before each `Dataset.pushData` call in the raw archive export:

1. Serialized size is estimated.
2. If the item exceeds the 8 MB safety limit after standard compaction (notes capped at 40, provenance IDs capped at 100, texts truncated), full audit detail is moved to KV under `RAW_ARCHIVE_AUDIT_<record_id>`.
3. The dataset item keeps a compact pointer plus summary.
4. If the item still exceeds 8 MB after KV offload, the run fails.

Review and match-ready exports are not subject to the same compaction — their records are small by construction.

## Normalization health checks

After each source is mapped and before dedupe:

- Logs total rows, rows with company name, rows with person name, rows with signal date, rows with all three, and 3 sample records.
- **Fails fast** if >90% of rows are missing `company_name` or `person_name` individually, or if >90% of rows are missing `signal_date`. This catches total mapping failures without rejecting real data that legitimately has some sparse rows.

## Match-ready gate

`match_ready` is intentionally strict. Records must pass all of:

1. `natural_person_confidence >= minNaturalPersonConfidence` (default 0.6)
2. `signal_confidence >= minSignalConfidence` (default 0.6)
3. `hasVerifiedContext`: `role` or `enrichment_context` must be non-empty
4. Signal type must not contain `unclear` or `unconfirmed`
5. For substantial holding reductions: `natural_person_confidence >= 0.75`

**`match_ready_records: 0` is a legitimate outcome.** It means all records were blocked by one or more of the above gates. This is not a pipeline failure. When Exa is disabled and source data contains no `role` field, the `hasVerifiedContext` gate will block all records — this is expected and explicitly logged.

## Actor input

Primary fields:

- `runAfmMar19` — enable MAR 19 source
- `runAfmSubstantialHoldings` — enable substantial holdings source
- `runExaConfirmation` — enable Exa context confirmation
- `afmMar19CsvUrl` — override MAR 19 endpoint URL
- `afmSubstantialHoldingsCsvUrl` — override substantial holdings endpoint URL
- `lookbackDays` — signals older than this many days score as stale (default 45)
- `minSignalConfidence` — minimum signal confidence for match-ready (default 0.6)
- `minNaturalPersonConfidence` — minimum natural person confidence for match-ready (default 0.6)
- `maxReviewRecords` — cap on review dataset size (default 100)
- `maxMatchReadyRecords` — cap on match-ready dataset size (default 30)
- `topBucketBForExa` — how many bucket B records to confirm with Exa (default 5)
- `exaApiKey` — Exa API key (resolved from input, then `EXA_API_KEY` env var)
- `debug` — enable verbose debug logging

### Exa API key resolution order

1. `input.exaApiKey`
2. `process.env.EXA_API_KEY`
3. If neither present, Exa confirmation is skipped cleanly. No error.

## Connect to Apify

1. Create a new Actor in Apify Console.
2. Choose **Source code → Git repository**.
3. Enter this repository URL.
4. Configure `EXA_API_KEY` as a secret environment variable if using Exa.

## Local validation

```bash
npm install
npm run build
npm test
npm run lint
```

## Observability

Look for these structured log markers in order:

```
actor initialized
input loaded
normalized runtime config
source modules selected
AFM MAR 19 fetch starting
AFM MAR 19 rows loaded
AFM substantial holdings fetch starting / degraded mode warning
AFM substantial rows loaded
source normalization health (per source)
dedupe started / completed
scoring started / completed
exports started
outputs written
actor exiting successfully / [ERROR] actor failed
```

The `RUN_SUMMARY` key-value entry is always written — even on failure (best-effort). It contains `final_run_state`, `degraded_run`, `source_status` per source (including row counts, retry counts, elapsed time, and error messages), record counts, and the `outputs_written` breakdown.

## Known limitations

- AFM source availability depends on upstream CSV endpoints. Substantial holdings endpoint intermittently returns 504.
- Exa confirmation is optional and confirmation-only. It does not override AFM source truth.
- The actor does not perform cross-run deduplication. Downstream consumers must handle records that may reappear across runs if they fall within the lookback window.

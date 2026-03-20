# Connector OS Dutch HNWI Signals

Apify actor that ingests AFM regulatory CSV exports (MAR 19 PDMR filings + substantial holdings), scores and filters Dutch HNWI liquidity signals, and writes analyst-ready outputs to Apify datasets and KV store.

---

## Apify operator guide

### 1. Connect this repository as a Git-based actor

1. Open [Apify Console](https://console.apify.com) → **Actors** → **Create new Actor**.
2. Under **Source**, choose **Git repository**.
3. Enter the repository URL.
4. Set **Branch** to `main` (or the branch you want to deploy from).
5. Leave **Directory** blank — the `Dockerfile` and `.actor/` are at the repository root.
6. Click **Save**.

The actor will auto-detect `.actor/actor.json` and use `Dockerfile` for the build.

### 2. Build the actor in Apify

1. In the actor's **Source** tab, click **Build**.
2. Apify runs `docker build` using the `Dockerfile` at the repository root.
3. The build runs `npm ci --include=dev` then `npm run build` (TypeScript compilation).
4. Expect the first build to take 60–120 seconds depending on npm cache warmth.

If the build fails, check the build log for TypeScript errors or missing `node_modules`. The `npm ci --include=dev` step must succeed before `npm run build`.

### 3. Set EXA_API_KEY in Apify

Exa confirmation is optional. To enable it:

**Option A — Actor input (recommended for per-run control):**
Set `exaApiKey` in the actor input. The field is marked `isSecret` in the input schema so Apify Console will mask it.

**Option B — Environment variable (recommended for always-on configuration):**
1. Go to the actor → **Environment variables** tab.
2. Add a new variable: **Name** = `EXA_API_KEY`, **Value** = your key.
3. Check **Secret value** so Apify stores it encrypted.

The actor resolves the key in this order:
1. `input.exaApiKey`
2. `process.env.EXA_API_KEY`
3. If neither is set, Exa confirmation is disabled cleanly — the run continues and `RUN_SUMMARY.exa_key_available` will be `false`.

**The raw key is never logged.** Only whether a key is available is logged.

### 4. Run the actor from Apify Console UI

1. Go to **Actors** → find your actor → **Run**.
2. The input form is generated from `.actor/input_schema.json`. All fields have defaults.
3. Minimum viable run (no customisation needed):
   - `runAfmMar19`: true
   - `runAfmSubstantialHoldings`: true
   - `runExaConfirmation`: false (or true if Exa key configured)
   - All other fields default.
4. Click **Start**.

### 5. What outputs to expect

| Output | Location | Content |
|--------|----------|---------|
| Raw archive | Default dataset | All scored signal records — one item per record |
| Review records | Named dataset `review` | Analyst-ranked subset, capped at `maxReviewRecords` (default 100) |
| Shortlist | Named dataset `shortlist` | Softer gate — bucket A/B, NPC ≥ 0.45, SC ≥ 0.40, capped at `maxShortlistRecords` (default 60) |
| Match-ready | Named dataset `match-ready` | Strict gate — all signal gates passed, capped at `maxMatchReadyRecords` (default 30) |
| Run summary | KV store → `RUN_SUMMARY` | JSON: final_run_state, source_status, record counts, output counts |
| Input schema | KV store → `INPUT_SCHEMA` | Runtime copy of the input schema used for this run |

Datasets are accessible in **Storage → Datasets** after the run. KV items are in **Storage → Key-value stores**.

**Normal outcome without Exa:** `match_ready_records` will be 0. This is expected. Match-ready requires `role` or `enrichment_context` data, which is only populated by Exa. The shortlist and review datasets will contain records.

**Normal outcome with Exa:** Match-ready records appear when Exa confirms person context. Expect a small number (0–10 per run) depending on signal quality.

### 6. Degraded mode

Degraded mode is entered when:
- AFM substantial holdings fetch fails after 2 retries (e.g. AFM 504 timeout)
- AND AFM MAR 19 fetch succeeded

In degraded mode:
- The run **continues** with MAR 19 data only.
- `RUN_SUMMARY.final_run_state` = `"degraded"`, `degraded_run` = `true`.
- `RUN_SUMMARY.source_status.afm_substantial.status` = `"degraded"` with the error message.
- All outputs are written with MAR 19 data only.
- The Apify run is **not** marked as failed.

To distinguish degraded from succeeded runs, check `RUN_SUMMARY.final_run_state` in KV.

### 7. Failure conditions

The run fails (non-zero exit, Apify marks run as **Failed**) when:
- AFM MAR 19 fetch fails after 1 retry (this is the primary fatal source)
- Both AFM sources fail simultaneously
- AFM MAR 19 returns data but a required column is absent (schema drift)
- An internal pipeline error is not caught and re-throws to the top level
- Raw archive contains an item that remains over 8 MB after compaction

When a run fails, a best-effort failure summary is written to `RUN_SUMMARY` in KV with `final_run_state: "failed"` and the `source_status` for each source including the error message.

### 8. Zero match-ready records — what it means

`match_ready_records: 0` is a **legitimate outcome**, not a pipeline failure.

Match-ready requires all of:
1. `natural_person_confidence >= minNaturalPersonConfidence` (default 0.6)
2. `signal_confidence >= minSignalConfidence` (default 0.6)
3. `role` or `enrichment_context` is non-empty (requires Exa enrichment)
4. Signal type must not contain `unclear` or `unconfirmed`
5. For substantial holding reductions: `natural_person_confidence >= 0.75`

Without Exa, gate 3 blocks all records. This is expected and logged explicitly. Check the **shortlist** dataset instead — it does not require enrichment context and will contain qualified signals.

### 9. Known limitations

- **AFM endpoint availability:** AFM substantial holdings endpoint intermittently returns HTTP 504. The actor retries 2 times with backoff. If all retries fail, degraded mode is entered automatically.
- **AFM MAR 19 is thin:** The MAR 19 export contains transaction date and party names but no transaction value, direction, or role. Signal direction is always `unclear` from this source alone.
- **No cross-run deduplication:** Records within the lookback window reappear on each run. Downstream consumers are responsible for deduplication across runs.
- **Exa requires external network access:** Exa API calls go to `api.exa.ai`. In network-restricted environments (e.g. sandboxes without external access) Exa calls will fail silently and confirmation will be skipped.
- **match_ready without Exa is always 0:** By design. Match-ready requires verified context that only Exa provides.
- **Large source memory:** The substantial holdings file is ~95 MB. The early lookback filter (applied immediately after parse) keeps in-memory records bounded. Runs with `lookbackDays > 180` on this source will retain more rows.

---

## Repository layout

```text
.actor/
  actor.json          # Apify actor manifest
  input_schema.json   # Apify Console input form definition
Dockerfile            # Build + runtime image
package.json
tsconfig.json
README.md
src/
  main.ts             # Actor entrypoint
  config.ts           # Default URLs and input defaults
  types.ts            # TypeScript contracts
  sources/            # AFM MAR 19, AFM substantial holdings, Exa
  normalize/          # Record normalization
  dedupe/             # Signal deduplication
  filters/            # Institutional filter, person confidence, signal gates
  scoring/            # NL relevance, issuer desirability, signal scoring, review priority
  enrich/             # Exa per-record enrichment + confirmation pass
  export/             # Raw archive, review dataset, match-ready, shortlist
  utils/              # CSV fetch/parse, dates, logging, strings
tests/
  fixtures/
  unit/
scripts/
  integration-run.ts  # Local integration runner (not used in Apify runtime)
```

---

## Source schema contract

Ingests AFM MAR 19 and AFM substantial holdings CSV feeds, applies normalization, filtering, scoring, deduplication, and export pipeline:

- **Default dataset**: raw archive of all processed records.
- **Named dataset `review`**: analyst review export, ranked by priority.
- **Named dataset `shortlist`**: softer-gated records eligible for outreach without enrichment.
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
3. **Retry-capable fetch**: the substantial holdings fetch uses `fetchWithRetry` with 2 retries and exponential backoff + jitter. MAR 19 fetch uses a single attempt — failure is immediately fatal.
4. **Bounded exports**: all dataset items are serialized and checked against an 8 MB safety limit before `Dataset.pushData`.

## Source reliability policy

| Source | On fetch failure |
|--------|-----------------|
| AFM MAR 19 | **No retry.** Failure is fatal — the run fails immediately. |
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

## Exa confirmation

When `runExaConfirmation: true` and an API key is available:

- All bucket A records and top `topBucketBForExa` bucket B records are submitted to Exa for context confirmation.
- Exa calls are processed in concurrent batches (`CONFIRMATION_CONCURRENCY = 5`) to avoid serial network bottleneck in Apify.
- Each record gets 2 Exa search passes (issuer domain + news) and 1 contents fetch.
- Confirmation sets: `context_confirmed`, `disposal_confirmed`, `role_confirmed`, `confirmation_evidence_strength`, `confirmation_summary`, `confirmation_urls`.
- A per-record Exa failure does not abort the confirmation pass — the record gets `confirmation_evidence_strength: none` and the run continues.
- If the Exa API is unreachable (network-restricted environment), all confirmations fail gracefully and the run continues.

## Actor input

Primary fields:

- `runAfmMar19` — enable MAR 19 source
- `runAfmSubstantialHoldings` — enable substantial holdings source
- `runExaConfirmation` — enable Exa context confirmation
- `afmMar19CsvUrl` — override MAR 19 endpoint URL
- `afmSubstantialHoldingsCsvUrl` — override substantial holdings endpoint URL
- `lookbackDays` — signals older than this many days score as stale (default 45)
- `minSignalConfidence` — minimum signal confidence gate (default 0.6)
- `minNaturalPersonConfidence` — minimum natural person confidence gate (default 0.6)
- `excludeInstitutions` — remove institutional records from all exports (default true)
- `maxReviewRecords` — cap on review dataset size (default 100)
- `maxMatchReadyRecords` — cap on match-ready dataset size (default 30)
- `maxShortlistRecords` — cap on shortlist dataset size (default 60)
- `topBucketBForExa` — how many bucket B records to confirm with Exa (default 5)
- `exaFreshnessMaxAgeHours` — Exa content max age in hours (default 72)
- `exaApiKey` — Exa API key (resolved from input, then `EXA_API_KEY` env var)
- `debug` — enable verbose debug logging

### Exa API key resolution order

1. `input.exaApiKey`
2. `process.env.EXA_API_KEY`
3. If neither present, Exa confirmation is skipped cleanly. No error.

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
exa key status
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

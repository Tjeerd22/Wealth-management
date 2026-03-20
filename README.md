# Connector OS Dutch HNWI Signals

This actor ingests two AFM CSV sources and applies a strict review pipeline without broadening source scope or loosening match-ready gates.

## Source schema contract

The actor now enforces explicit source-specific CSV contracts immediately after parsing.

### AFM MAR 19 required columns

- `Transactie`
- `Uitgevende instelling`
- `Meldingsplichtige`
- `MeldingsPlichtigeAchternaam`

### AFM substantial holdings required columns

- `Datum meldingsplicht`
- `Uitgevende instelling`
- `Meldingsplichtige`
- `Kvk-nr`
- `Plaats`

If a required column is missing, the run fails fast with a clear error. The runtime also logs the parsed columns and any unexpected extra columns for operational inspection.

## Source-specific mapping

The actor does not rely on generic header guessing.

### AFM MAR 19 mapping

- `Transactie -> signal_date_raw`
- `Uitgevende instelling -> company_name`
- `Meldingsplichtige -> person_name`
- `MeldingsPlichtigeAchternaam -> person_last_name`

### AFM substantial holdings mapping

- `Datum meldingsplicht -> signal_date_raw`
- `Uitgevende instelling -> company_name`
- `Meldingsplichtige -> person_name`
- `Kvk-nr -> kvk_number`
- `Plaats -> city`

Ownership percentages are kept only in compact source metadata plus canonical percentage fields.

## Normalization health checks

For each source the actor logs:

- total rows
- rows with `company_name`
- rows with `person_name`
- rows with `signal_date`
- rows with all required identity fields
- three sample normalized records

The actor fails fast when more than 90% of rows are missing `company_name` or `person_name` after source-specific mapping.

## Dedupe invariants

Dedupe is intentionally conservative and identity-safe.

- Unknown identities do not share a dedupe key.
- Missing `signal_date` prevents shared dedupe keys.
- Distinct dates remain distinct.
- Records are not merged by issuer-only or surname-only heuristics.
- The runtime logs first keys per source, before/after counts, merge counts, top merge reasons, suspicious groups, and an implausible reduction warning.

## Source reliability and degraded mode

### Required source behavior

- If AFM MAR 19 fails, the run fails.
- AFM substantial holdings retries up to 2 times on timeout or HTTP 5xx.
- If AFM substantial holdings still fails after those retries, the run may continue in degraded mode using MAR 19 only.
- If both sources fail, the run fails.

### Degraded-mode meaning

A degraded run is allowed only when:

- AFM MAR 19 succeeded, and
- AFM substantial holdings exhausted its retry policy on a retryable failure, and
- outputs are still written successfully.

The run summary records:

- `run_state`
- `degraded_run`
- per-source `source_status`
- reduced source coverage through row counts and source status details

## Truthful run outcomes

The actor uses explicit final states:

- `failed`
- `degraded`
- `succeeded`

Internal pipeline failure is rethrown and fails the run unless the run intentionally entered degraded mode and still wrote valid outputs.

## Export size bounds

Raw archive dataset items are bounded before every dataset write.

- Serialized item size is estimated before write.
- Notes, provenance IDs, provenance sources, confirmation URLs, and confirmation sources are capped.
- Large text summaries are truncated.
- If an item still remains too large, oversized audit detail is moved to KV store under `RAW_ARCHIVE_AUDIT_<record_id>` and the dataset item is compacted to a safe pointer.
- Compaction is logged.

## Large-source handling

AFM substantial holdings is treated as a large source.

- Source-specific canonical records stay compact.
- No giant array spreads are used for source merging.
- Provenance and notes growth are bounded during export.
- The actor avoids carrying unnecessary raw payload structures through the pipeline.

## Runtime log sequence

Expected operational log sequence:

- `actor initialized`
- `input loaded`
- `normalized runtime config`
- `source selected`
- `source fetch started`
- `source fetch completed`
- `source parse completed`
- `source normalization health`
- `dedupe started`
- `dedupe completed`
- `scoring started`
- `scoring completed`
- `exports started`
- `outputs written`
- `final run state`

## Run summary fields

The run summary includes:

- `degraded_run`
- `source_status`
- `raw_records`
- `post_filter_records`
- `review_records`
- `match_ready_records`
- `excluded_institutions`
- `low_confidence_records`
- `review_bucket_stats`
- `outputs_written`

## Local validation

```bash
npm install
npm test
npm run lint
npm run build
```

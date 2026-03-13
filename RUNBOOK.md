# RUNBOOK

## Objective
Run the demand engine to generate a shortlist of 100-200 Dutch issuer opportunities from public ownership/liquidity signals.

## Setup
1. Import `supabase/schema.sql` into Supabase.
2. Import all workflow JSON files from `n8n/workflows`.
3. Set n8n environment variables:
   - `SUPABASE_URL`
   - `SUPABASE_SERVICE_ROLE_KEY`
   - `EXA_API_KEY` (optional)
4. Ensure code nodes use the embedded parser/normalizer/scoring logic shipped in these workflow files.

## Recommended activation sequence
1. Activate `AFM MAR19 Ingest`.
2. Activate `AFM Substantial Holdings Ingest`.
3. Activate `Euronext Company News Ingest`.
4. Activate `Scoring and Clustering`.

## Data contract
- Every ingestion lane writes raw records into `raw_events`.
- Every lane normalizes records into `normalized_events`.
- Scoring writes idempotently into `scored_events`.
- Clustering writes issuer-level output into `issuer_opportunities`.

## Quality gates
- Keep threshold in scoring is deterministic (`total >= 6`) and requires no rejection reason.
- Issuer shortlist gate in clustering:
  - minimum 2 kept events per issuer
  - minimum aggregate score 12
  - capped to top 200 issuers per run
- Institutional holder and technical-only events are explicitly down-ranked/rejected.

## Weekly operating routine
1. Review top 20 `issuer_opportunities` records for explainability.
2. Tighten keywords/thresholds if false positives appear.
3. Confirm AFM/Euronext selectors still match source pages.
4. Backfill missing event context fields if an upstream format changes.

## Troubleshooting
- If no events arrive:
  - verify source URLs still return parseable payloads.
  - check n8n node execution output for selector drift.
- If duplicates appear:
  - verify `row_hash` is present before raw upsert.
  - verify upsert URLs include `on_conflict` parameters.
- If shortlist quality drops:
  - inspect `rejection_reason` and `explanation` in `scored_events`.
  - tune subclass/direction parser and deterministic thresholds before changing AI prompts.

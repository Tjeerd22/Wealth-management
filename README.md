# Connector OS Dutch Liquidity Signals Actor v1

Production-minded Apify actor for monthly Dutch HNWI / liquidity-event signal qualification. This v1 is intentionally narrow: it ingests AFM MAR 19 CSV data, AFM substantial holdings CSV data, and optionally adds lightweight Exa enrichment.

## What this actor does

- Favors timing-based wealth transition signals over static rich lists.
- Treats AFM CSV exports as the primary evidence layer.
- Heavily filters institutional noise, especially in substantial holdings data.
- Produces three operator-facing outputs:
  - raw signal archive
  - human review dataset
  - match-ready dataset for Connector OS
- Uses transparent rule-based scoring and gating.
- Prefers false negatives over false positives.

## Project structure

```text
src/
  main.ts
  inputSchema.ts
  config.ts
  types.ts
  sources/
    afmMar19.ts
    afmSubstantialHoldings.ts
    exaEnrichment.ts
  normalize/
    normalizeRecord.ts
  filters/
    institutionalFilter.ts
    personConfidence.ts
    signalGates.ts
  dedupe/
    dedupeSignals.ts
  scoring/
    scoreSignal.ts
  enrich/
    enrichRecord.ts
  export/
    exportRawArchive.ts
    exportReviewDataset.ts
    exportMatchReady.ts
  utils/
    csv.ts
    dates.ts
    strings.ts
    logging.ts
tests/
  fixtures/
  unit/
```

## Input schema

```json
{
  "runAfmMar19": true,
  "runAfmSubstantialHoldings": true,
  "runExaEnrichment": false,
  "afmMar19CsvUrl": "",
  "afmSubstantialHoldingsCsvUrl": "",
  "lookbackDays": 45,
  "minSignalConfidence": 0.6,
  "minNaturalPersonConfidence": 0.6,
  "excludeInstitutions": true,
  "maxReviewRecords": 100,
  "maxMatchReadyRecords": 30,
  "exaApiKey": "",
  "debug": false
}
```

If AFM URLs are omitted, the actor falls back to internal defaults configured in `src/config.ts`.

## Pipeline order

1. Scaffold config and input.
2. Ingest AFM MAR 19 records as thin but timely `pdmr_transaction_unconfirmed` signals.
3. Ingest AFM substantial holdings records as either `substantial_holding_reduction` or `substantial_holding_change_unclear`.
4. Normalize all records into a shared schema.
5. Apply explainable institutional filtering.
6. Score natural-person confidence.
7. Enrich with domain heuristics and optional Exa support.
8. Score signal confidence.
9. Apply hard gates before anything becomes `match_ready`.
10. Deduplicate deterministically.
11. Export raw, review, and match-ready outputs.

## Scoring logic

### Natural person confidence

Transparent heuristics score from 0 to 1 using:
- plausible full-name structure
- initials plus surname handling
- entity-like terms and legal suffix penalties
- institutional-risk penalties
- role-based and enrichment-based boosts

### Signal confidence

Weighted components:
- source quality
- evidence strength
- recency
- natural person confidence
- signal type strength
- enrichment quality

Important caps:
- MAR 19 records stay capped because the export does not confirm disposal.
- Obvious institutional substantial holdings records are capped low.
- Weak / unclear signals remain review-only.

## Output design

- **Default dataset**: raw archive of all normalized records after filtering notes and scoring.
- **Named dataset `review`**: analyst review dataset capped by `maxReviewRecords`.
- **Named dataset `match-ready`**: only records that pass all gates, capped by `maxMatchReadyRecords`.
- **Key-value store**:
  - `RUN_SUMMARY`
  - `INPUT_SCHEMA`

## Run instructions

```bash
npm install
npm run build
npm test
npm start
```

To run on Apify, configure actor input and provide an `exaApiKey` only if Exa enrichment is desired.

## Testing coverage

Unit tests cover:
1. clear institution record from substantial holdings
2. likely natural person from MAR 19
3. initials plus surname case
4. Dutch surname prefix case
5. duplicate merge case
6. ambiguous case that must remain review-only
7. record that passes all gates into match-ready
8. record capped due to insufficient evidence

## Fully implemented

- AFM MAR 19 CSV ingestion.
- AFM substantial holdings CSV ingestion.
- Shared normalization schema.
- Explainable institutional filtering and natural-person scoring.
- Transparent rule-based signal scoring and gating.
- Deterministic-first dedupe.
- Raw, review, and match-ready exports.
- Optional Exa enrichment support.
- Unit tests and realistic CSV fixtures.

## Stubbed

- Exa enrichment remains lightweight and only uses a simple keyword search request.
- Company domain inference is heuristic-first, not verified.
- Source default URLs are placeholders that may need confirmation against current AFM export endpoints.

## Intentionally postponed

- Full Euronext crawling.
- Full news-site crawling.
- KvK scraping.
- LinkedIn scraping.
- Google News scraping.
- Social media scraping.
- Manual analyst override file.
- Additional enrichment providers.
- ML ranking models.

## Known v1 limitations

- AFM MAR 19 exports are inherently thin and do not prove disposal.
- Substantial holdings data is institution-heavy; family vehicles can still require human review.
- Domain inference may produce false positives for uncommon company names.
- Exa enrichment is optional and non-authoritative.
- Final outreach approval and identity resolution remain manual.

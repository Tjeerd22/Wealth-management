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

### Tested AFM defaults

The placeholder defaults have been replaced with the live AFM export endpoints currently linked from the AFM register pages:

- MAR 19 CSV: `https://www.afm.nl/export.aspx?format=csv&type=0ee836dc-5520-459d-bcf4-a4a689de6614`
- Substantial holdings CSV: `https://www.afm.nl/export.aspx?format=csv&type=1331d46f-3fb6-4a36-b903-9584972675af`

The AFM register pages showing these export links were confirmed on **19 March 2026**:

- MAR 19 register page: `https://www.afm.nl/en/sector/registers/meldingenregisters/transacties-leidinggevenden-mar19-`
- Substantial holdings register page: `https://www.afm.nl/en/sector/registers/meldingenregisters/substantiele-deelnemingen`

In this environment, direct network fetches to AFM CSV downloads were blocked by the outbound proxy. For repeatable validation, the repository now includes a **page-derived current audit snapshot** under `audit_inputs/` using current 19 March 2026 records copied from the live AFM register pages.

## Pipeline order

1. Scaffold config and input.
2. Ingest AFM MAR 19 records as thin but timely `pdmr_transaction_unconfirmed` signals.
3. Ingest AFM substantial holdings records as either `substantial_holding_reduction` or `substantial_holding_change_unclear`.
4. Normalize all records into a shared schema.
5. Apply explainable institutional filtering.
6. Score natural-person confidence.
7. Enrich with domain heuristics and optional Exa support.
8. Score Dutch wealth-management relevance (`nl_relevance_score`).
9. Score signal confidence with wider spread across person quality, NL relevance, issuer relevance, evidence quality, and context quality.
10. Apply hard gates before anything becomes `match_ready`, while recording explicit `blocked_by` reasons.
11. Classify non-match-ready records into review buckets `A`, `B`, and `C`.
12. Deduplicate deterministically without collapsing distinct same-person events on different dates.
13. Export raw, review, and match-ready outputs.

## Scoring logic

### Natural person confidence

Transparent heuristics score from 0 to 1 using these practical bands:
- `0.70` to `0.85`: clear personal names or human comma-prefix patterns
- `0.50` to `0.65`: initial-plus-surname or surname-plus-initial patterns without entity signals
- `0.25` to `0.45`: ambiguous tokenization or single-token names
- `0.00` to `0.15`: clear institutions, legal entities, or trading-style names

### Netherlands relevance

`nl_relevance_score` is a transparent ranking feature from 0 to 1. It is **not** a hard filter. It increases when the record shows:
- Dutch issuer or strong Netherlands nexus
- Euronext Amsterdam / Dutch-listed relevance
- plausible Dutch executive or tax relevance
- issuer patterns more likely to matter for Dutch boutique wealth-management workflows

### Signal confidence

Weighted components now emphasize score spread across:
- source quality
- evidence strength
- recency
- natural person confidence
- `nl_relevance_score`
- signal type strength
- issuer relevance
- context quality

Important caps:
- MAR 19 records stay capped because the export does not confirm disposal.
- Obvious institutional substantial holdings records are capped low.
- Weak / unclear signals remain review-only.

## Output design

- **Default dataset**: raw archive of all normalized records after filtering notes and scoring.
- **Named dataset `review`**: analyst review dataset capped by `maxReviewRecords`, sorted by `review_bucket` first and then confidence.
- **Named dataset `match-ready`**: only records that pass all gates, capped by `maxMatchReadyRecords`.
- Every reviewable record now includes:
  - `nl_relevance_score`
  - `blocked_by` as an explicit structured array
  - `review_bucket` with values `A`, `B`, or `C`
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
2. likely natural person from MAR 19 in the stronger human-name band
3. initials plus surname case scored as moderate confidence
4. Dutch surname prefix case
5. duplicate merge case for same-day name variants
6. exact-duplicate removal without collapsing distinct dated events
7. ambiguous case that must remain review-only with explicit blockers
8. record that passes all gates into match-ready
9. score-spread behavior and blocker transparency for MAR 19
10. review export ordering by bucket and then confidence

## Fully implemented

- AFM MAR 19 CSV ingestion.
- AFM substantial holdings CSV ingestion.
- Shared normalization schema.
- Explainable institutional filtering and natural-person scoring.
- Transparent rule-based signal scoring and gating.
- Deterministic-first dedupe with provenance preservation.
- Raw, review, and match-ready exports.
- Structured blocker tracking and review bucketing.
- Optional Exa enrichment support.
- Unit tests and realistic CSV fixtures.
- Audit snapshot exports in `audit_outputs/`.

## Stubbed

- Exa enrichment remains lightweight and only uses a simple keyword search request.
- Company domain inference is heuristic-first, not verified.
- Direct AFM download execution can still depend on the runtime network environment.

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

## 19 March 2026 audit results

### Validation scope

- Source URLs were verified from the live AFM register pages on **19 March 2026**.
- Because this environment could not directly fetch AFM CSV downloads through the proxy, the actor was validated against a current **page-derived audit snapshot**:
  - `audit_inputs/afm_mar19_current_2026-03-19.csv`
  - `audit_inputs/afm_substantial_current_2026-03-19.csv`
- Snapshot size:
  - 50 current MAR 19 rows
  - 50 current substantial holdings rows

### Observed output volumes

Observed on the 19 March 2026 current audit snapshot after the iteration-2 ranking and review updates in this repository:

- Raw source rows: 100 total
  - AFM MAR 19: 50
  - AFM substantial holdings: 50
- Raw records after dedupe: 97
- Post-filter records: 42
- Excluded institutions: 55
- Review records exported: 30
- Match-ready records exported: 0
- Review bucket distribution after post-filtering:
  - `A`: 1
  - `B`: 30
  - `C`: 11

The zero `match-ready` outcome remains intentional for this validation slice: the current evidence is commercially safer as review-only than as outreach-ready.

### False-positive categories found during audit

The audit found these recurring false-positive categories and tightened v1 accordingly:

1. **Institutional holder names in substantial holdings**
   - Examples: BlackRock, UBS Group AG, Bank of America Corporation, DWS Investment GmbH, Goldman Sachs Group Inc.
   - Action: expanded institutional-name patterns and excluded these records earlier.

2. **Trading-style or vehicle-style names inside MAR 19**
   - Examples: `Summit Place 20 CC (trading as Foxhole Capital)`, `Icecat International B.V.`
   - Action: hard penalties for digits, parentheses, `trading as`, and legal-entity suffixes in notifying-party names.

3. **Single-token or weakly identified names**
   - Examples: `Westerling`, `Kakkad`
   - Action: reduced natural-person confidence for single-token names.

4. **Initial-only or initial-heavy names that look like real people but remain too thin**
   - Examples: `Bayoglu U.`, `Bounds P.`, `Kobel T.`
   - Action: they are now scored as moderate human evidence instead of near-failures, but they still remain review-only unless stronger verified context exists.

5. **Over-optimistic domain/context use**
   - Previous behavior treated a heuristically inferred company domain as enough context.
   - Action: inferred domains still help operator workflow, but they no longer count as verified context for `match-ready`.

6. **Thin AFM evidence being scored too generously**
   - MAR 19 and unclear substantial-holdings signals could previously drift too close to outreach readiness.
   - Action: unconfirmed or unclear signals are now capped lower and forced to review-only.

### Known remaining false-positive patterns

Even after tightening, analysts should still expect review noise from:

- Initial-plus-surname MAR 19 records with no verified role context.
- Single-word surnames where AFM does not expose enough identity detail.
- Family-office or family-vehicle names that omit obvious institutional keywords.
- Legitimate natural persons reported through legal vehicles in substantial holdings data.

### Recommended manual review rules before outreach

Before any outreach, require all of the following:

1. Confirm the notifying party is a natural person, not a legal vehicle or nominee.
2. Confirm the signal is a real disposal / liquidity indicator, not merely an unclear threshold update.
3. Verify role context from a first-party company biography, annual report, or equivalent authoritative source.
4. Confirm the inferred company domain manually; do not trust heuristic domain guesses on their own.
5. Reject records with only initials unless a second source resolves the identity unambiguously.
6. Reject records containing legal suffixes, fund/bank terms, `trading as`, or entity-style punctuation unless manually cleared.
7. Treat family holding structures as analyst-review-only until beneficial ownership is explicit.

### Before / after audit comparison

- Before this iteration, the top review rows clustered around `natural_person_confidence = 0.20` and `signal_confidence = 0.56`, which made obviously human MAR 19 names look too weak.
- After this iteration, surname-plus-initial and initial-plus-surname MAR 19 names land in the moderate human-evidence band instead of near-failure, while `blocked_by` makes the remaining review-only reasons explicit.
- Ranking now spreads review records by bucket and Dutch relevance rather than treating most thin MAR 19 rows as effectively tied.
- `match_ready` remains at `0`, so the actor did not inflate outreach eligibility to manufacture better-looking output.

### Audit exports

- Run summary: `audit_outputs/run_summary.json`
- Top 30 review records: `audit_outputs/top30_review.json`
- Top 30 match-ready records: `audit_outputs/top30_match_ready.json`
- Local rerun helper used for the repository audit snapshot: `scripts_rerun_audit.ts`

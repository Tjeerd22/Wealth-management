# Connector OS Dutch HNWI Signals

This repository is now structured so it can be connected directly to Apify as a **Git-based Actor source**. The actor keeps the existing Dutch HNWI signal business logic intact and only adjusts project structure, configuration, and runtime compatibility needed for Apify builds.

## Apify actor structure

The repository root now contains the files Apify expects for Git-based Actor builds:

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
```

## What the actor does

The actor ingests AFM MAR 19 and AFM substantial holdings CSV feeds, applies the existing normalization, filtering, scoring, deduplication, and export pipeline, and writes Apify-friendly outputs:

AFM exports may arrive as semicolon-delimited CSV with a UTF-8 BOM, so the shared ingestion layer now explicitly tries semicolon parsing first, falls back to comma parsing when needed, and logs the selected delimiter plus parsed row counts.

The AFM-only runtime is also hardened for large real-world AFM exports: source-record merging now uses iterative appends instead of spread-based bulk pushes, which prevents stack overflows when substantial holdings files reach 250k+ rows.

- **Default dataset**: raw archive of processed records.
- **Named dataset `review`**: analyst review export.
- **Named dataset `match-ready`**: strict match-ready export.
- **Default key-value store**:
  - `RUN_SUMMARY`
  - `INPUT_SCHEMA`

## Actor input in Apify

The Apify Console input schema includes these primary fields:

- `runAfmMar19`
- `runAfmSubstantialHoldings`
- `runExaConfirmation`
- `afmMar19CsvUrl`
- `afmSubstantialHoldingsCsvUrl`
- `lookbackDays`
- `minSignalConfidence`
- `minNaturalPersonConfidence`
- `maxReviewRecords`
- `maxMatchReadyRecords`
- `topBucketBForExa`
- `exaApiKey` (secret)
- `debug`

### EXA API key behavior

The runtime resolves the Exa key in this order:

1. `input.exaApiKey`
2. `process.env.EXA_API_KEY`
3. if neither is present, Exa confirmation is skipped cleanly

That means you can either:

- paste the key into the Actor input as `exaApiKey`, or
- configure `EXA_API_KEY` as a secret environment variable in Apify Console

## Connect this repo to Apify as a Git source

In the Apify Console:

1. Create a new Actor.
2. Choose **Source code → Git repository**.
3. Enter the repository URL for this repo.
4. If you need a non-default branch, set the branch in the Git source settings.
5. For monorepos, Apify supports `branch:path` selection. This repository is not a monorepo, so use the repository root as the source path.

### Git source examples

- Repo URL: `https://github.com/<org>/<repo>.git`
- Optional monorepo-style source selector example: `main:actors/connector-os-dutch-hnwi-signals`

For this repository, point Apify at the repo root because the Actor files live at the top level.

## Build and run on Apify

Apify reads `.actor/actor.json`, uses the root `Dockerfile`, installs dependencies, builds TypeScript, and starts the compiled Actor from `dist/main.js`.

### Local validation

```bash
npm install
npm run build
npm test
npm run lint
npm start
```

## Configure the Actor in Apify

### Option A: provide Exa key in input

Paste a value into the secret input field:

```json
{
  "runAfmMar19": true,
  "runAfmSubstantialHoldings": true,
  "runExaConfirmation": true,
  "topBucketBForExa": 5,
  "exaApiKey": "YOUR_EXA_KEY"
}
```

### Option B: configure `EXA_API_KEY` as an Apify secret environment variable

In the Apify Console:

1. Open the Actor.
2. Go to **Settings**.
3. Add environment variable `EXA_API_KEY`.
4. Mark it as secret.
5. Run the Actor with `runExaConfirmation` enabled.


## Runtime troubleshooting

### Build succeeds but run does nothing

If the Actor build passes but the run exits after the Apify system banner, inspect the runtime stage logs from `src/main.ts`. A real run now emits an explicit startup path covering actor init, input resolution, source selection, both AFM fetch stages, normalization, dedupe, scoring, export, output writes, and successful exit.

### Where to look in logs

Look for these runtime markers in order:

- `actor initialized`
- `input loaded`
- `normalized input resolved`
- `source modules selected`
- `AFM MAR 19 fetch starting`
- `AFM MAR 19 rows loaded`
- `AFM substantial holdings fetch starting`
- `AFM substantial rows loaded`
- `starting merge of source records`
- `merge completed`
- `normalization started`
- `normalization completed`
- `dedupe started`
- `dedupe completed`
- `scoring started`
- `scoring completed`
- `exports started`
- `outputs written`
- `actor exiting successfully`

The Actor also prints a normalized runtime configuration snapshot with secrets redacted to a boolean presence flag, and it now fails fast if no source module is enabled or if both AFM source URLs normalize to empty values.

### Outputs that should exist after a real run

After a successful run, you should always see:

- the **default dataset** populated with the raw archive export when any records were processed
- the **default key-value store** containing `RUN_SUMMARY`
- the **default key-value store** containing `INPUT_SCHEMA`
- the named **`review`** dataset created by the review export path
- the named **`match-ready`** dataset created by the match-ready export path

If a source fetch returns zero rows, the logs now call that out explicitly so an empty run is observable instead of silent.

## Expected outputs

After a run, expect:

- **Dataset**: raw archive records
- **Dataset `review`**: review candidates sorted by the existing ranking logic
- **Dataset `match-ready`**: only records that satisfy the unchanged strict gating logic
- **Key-value store `RUN_SUMMARY`**: aggregate run counts
- **Key-value store `INPUT_SCHEMA`**: runtime copy of the input schema

## Known limitations

- AFM source availability depends on the upstream CSV endpoints.
- Exa confirmation is optional and remains a confirmation/context layer only.
- If no Exa API key is configured, Exa confirmation is skipped without failing the run.
- Existing business rules, match-ready strictness, and data sources are intentionally unchanged.


## Dedupe invariants

The AFM-only dedupe stage is intentionally conservative:

- Exact merges require the same normalized person name, normalized issuer name, signal date, signal type, and source.
- Initial/surname merges are only allowed when the source, issuer, signal type, and exact date also match.
- Distinct dates are never merged.
- Dedupe groups do not chain across already-merged aliases; each record is matched only against a stable per-key canonical record.
- The runtime logs records before dedupe, records after dedupe, merge counts, top merge reasons, suspiciously large groups, and a warning when the reduction ratio looks implausibly high.

## Raw archive size limits

The default dataset is now bounded before each `Dataset.pushData` call:

- Each raw archive item is serialized and checked against an 8,000,000-byte safety limit.
- Oversized `notes`, `provenance_record_ids`, `provenance_sources`, `confirmation_urls`, and `confirmation_sources` arrays are capped.
- `signal_detail`, `raw_source_payload_summary`, `confirmation_summary`, and `enrichment_context` are truncated when needed.
- If a record is still too large after normal compaction, the full audit detail is moved to the default key-value store under `RAW_ARCHIVE_AUDIT_<record_id>` and the dataset item keeps only a compact pointer plus summary.
- Review and match-ready exports keep their existing semantics; only the raw archive representation is compacted for storage safety.

## Remaining manual steps in Apify Console

1. Create the Actor from this Git repository.
2. Confirm the build succeeds.
3. Configure either `exaApiKey` in Actor input or `EXA_API_KEY` in environment variables.
4. Run a test execution from the Apify UI.
5. Review the default dataset, `review` dataset, `match-ready` dataset, and `RUN_SUMMARY` in the default key-value store.

# Wealth Demand Engine Build Pack

This pack is designed to help you build a Dutch wealth-management demand engine around public liquidity and ownership signals.

## What this pack includes

- `AGENTS.md`
  Guidance for Codex or any coding agent working in this project.
- `docs/architecture.md`
  Full system design, source lanes, and build order.
- `supabase/schema.sql`
  Database schema for raw events, normalized events, scored events, issuer opportunities, and contact enrichment.
- `n8n/workflows/*.json`
  Importable starter workflows for:
  - AFM MAR 19 ingestion
  - AFM substantial holdings ingestion
  - Euronext company news ingestion
  - Issuer scoring and clustering
- `n8n/code/*.js`
  Code for event normalization and scoring.
- `prompts/*.txt`
  Tight AI prompts for qualification and outreach angles.
- `examples/codex-task.md`
  A ready-to-paste Codex task prompt.
- `RUNBOOK.md`
  Operational setup, quality gates, and troubleshooting for running the engine.

## The actual output you are building

You are not building a list of people.
You are building a list of issuers that show public signals which may imply a wealth transition for insiders, founders, directors, or large holders.

The engine should produce a rolling shortlist of roughly 100 to 200 Dutch-relevant demand issuers over a 60 to 120 day window.

## Recommended build order

1. Import the Supabase schema.
2. Import the n8n workflows.
3. Add credentials to the HTTP, Supabase, and optional Exa nodes.
4. Paste the code from `n8n/code/*.js` into the relevant Code nodes.
5. Run AFM MAR 19 and AFM substantial holdings first.
6. Only after that, enable Euronext and optional Exa lanes.
7. Review the `issuer_opportunities` table weekly and tighten your scoring thresholds.

## Environment variables you will likely need

For n8n:
- `SUPABASE_URL`
- `SUPABASE_SERVICE_ROLE_KEY`
- `EXA_API_KEY` if you use the private-liquidity lane

For local development or Codex:
- `SUPABASE_URL`
- `SUPABASE_ANON_KEY`
- `SUPABASE_SERVICE_ROLE_KEY`
- `EXA_API_KEY`

## Notes on the starter workflows

These workflows are meant as strong starting templates with improved deterministic parsing and scoring.
You should still:
- confirm the exact AFM export URLs you use
- periodically verify Euronext selectors and extraction strategy
- connect your own enrichment provider if you want LinkedIn or email discovery
- tune score thresholds against live data and false-positive rates

## Suggested next move with Codex

Open Codex in a repo folder, select this project, and ask it to:

1. validate the n8n workflow JSON
2. wire in the supplied JS files into the Code nodes
3. add tests for the scoring logic
4. create a small local parser for sample AFM export files
5. produce a `docker-compose.yml` or local runbook if you want a reproducible environment

You can use the prompt in `examples/codex-task.md`.

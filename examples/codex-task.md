# Codex task

You are working inside a repo that contains a Dutch wealth-demand engine build pack.

## Objective

Turn this pack into a working local project and validate the n8n starter workflows.

## Tasks

1. Validate all JSON files under `n8n/workflows`.
2. Validate the SQL in `supabase/schema.sql`.
3. Review the JS files in `n8n/code` and improve them only where the logic is clearly broken.
4. Create a small `tests/` folder with sample fixtures for:
   - AFM MAR 19
   - AFM substantial holdings
   - Euronext company news
5. Add a lightweight script that can normalize and score sample records outside n8n for local testing.
6. Add a short `RUNBOOK.md` with local setup and troubleshooting steps.
7. Keep the architecture and table names stable unless there is a strong reason to change them.

## Constraints

1. Do not weaken the false-positive discipline.
2. Prefer explicit, readable logic over clever abstractions.
3. Do not turn this into a generic scraping framework.
4. Preserve the goal: rank issuers with wealth-transition signals.

## Deliverables

- working sample parser
- improved tests
- validated workflow files
- runbook

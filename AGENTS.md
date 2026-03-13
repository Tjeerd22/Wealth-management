# AGENTS.md

## Goal

Build and maintain a Dutch wealth-management demand engine that turns public ownership and liquidity signals into a shortlist of issuers worth targeting.

## Do not change the core goal

The output is a ranked issuer list, not a generic people list.

## Primary source lanes

1. AFM MAR 19
2. AFM substantial holdings
3. Euronext company news
4. Optional AFM inside information
5. Optional Exa private-liquidity lane

## Important principles

1. False positives are worse than false negatives.
2. Use rule-based scoring before AI classification.
3. Do not classify solely from names or titles. Use event context.
4. Favor issuer-level clustering over raw event volume.
5. Exclude institutional ownership noise aggressively.
6. Treat MAR 19 index records as triggers, not final evidence.

## Event model

Every raw event should normalize into:
- source
- source_type
- event_date
- issuer_name
- person_name
- event_class
- event_subclass
- transaction_direction
- price
- quantity
- currency
- estimated_value
- role_title
- detail_url
- raw_text

## Keep logic

Keep events when there is evidence of:
- disposal
- sale
- ownership reduction
- major holding reduction
- M&A transfer
- secondary placement
- block sale
- other likely wealth-transition events

Reject or down-rank:
- grant
- award
- purely technical exercise
- option conversions without sale
- institutional passive position changes
- generic governance noise

## Build behavior

When asked to change code:
1. preserve existing schema names where possible
2. add comments only where the logic is non-obvious
3. prefer explicit thresholds over hidden heuristics
4. keep AI prompts short and hard-edged
5. do not over-abstract a simple parser

## Validation

Before shipping changes:
1. validate all JSON files
2. run JS linting if available
3. test score-event logic on at least 5 representative cases
4. confirm that the top 20 issuer opportunities are explainable by a human

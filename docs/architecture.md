# Architecture

## Objective

Build a demand engine that identifies Dutch-relevant issuers showing public signals of wealth transition, liquidity, or ownership change that can be relevant for wealth-management outreach.

## Core thesis

The market is not rich people in the Netherlands.
The market is publicly visible wealth transition moments connected to issuers and identifiable people.

## Output target

A rolling 60 to 120 day shortlist of roughly 100 to 200 issuers, ranked by event strength and outreach relevance.

## Source lanes

### Lane 1: AFM MAR 19
Use as a trigger lane for insider events.
Do not trust the list page alone.
Open detail pages and parse transaction rows.

### Lane 2: AFM substantial holdings
Use as a stronger ownership-shift lane.
Compute deltas for capital interest and voting rights.

### Lane 3: Euronext company news
Use as context and confirmation.
Focus on:
- Major shareholding notifications
- Mandatory notification of trade primary insiders
- Mergers, acquisitions, transfers
- Other financial transaction
- Takeover bids
- Share introduction and issues

### Lane 4: AFM inside information
Optional context lane.
Only use filtered titles and texts that imply transactions or ownership events.

### Lane 5: Exa private-liquidity lane
Optional lane for private or semi-private exits not visible in AFM.

## Data flow

### A. Source ingestion
Each source writes to `raw_events`.

### B. Normalization
All sources normalize into `normalized_events`.

### C. Detail enrichment
Open the best candidate detail pages and extract structured context.

### D. Rule-based scoring
Write score, keep flag, and explanation to `scored_events`.

### E. Issuer clustering
Aggregate multiple events into `issuer_opportunities`.

### F. Contact enrichment
Resolve target people or routes into `demand_targets`.

## Scoring philosophy

Use rule-based logic first.
AI only acts as a tight classifier after the rule score exists.

### Typical positive factors
- disposal or sale
- reduction in capital interest or voting rights
- higher estimated value
- founder, director, officer, or large holder context
- confirmation across multiple lanes

### Typical negative factors
- grant or award
- pure exercise with no sale
- passive institutional holder noise
- buyback boilerplate
- generic governance updates

## Issuer-level clustering

An issuer becomes an opportunity when:
- it has sufficient total score in a rolling window
- the signal is explainable by a human
- the event has a plausible wealth-transition angle
- it is not purely institutional noise

## Build sequence

### Phase 1
AFM MAR 19 ingestion
AFM substantial holdings ingestion
Supabase schema
Normalization and dedupe

### Phase 2
MAR 19 detail scraping
Substantial holdings delta engine
Scoring

### Phase 3
Euronext scrape
Issuer clustering

### Phase 4
Exa lane
Contact enrichment
Operational shortlist

create extension if not exists pgcrypto;

create table if not exists raw_events (
  id uuid primary key default gen_random_uuid(),
  source text not null,
  source_type text not null,
  fetched_at timestamptz not null default now(),
  external_id text,
  event_date date,
  issuer_name text,
  person_name text,
  title text,
  raw_text text,
  detail_url text,
  payload_json jsonb not null default '{}'::jsonb,
  row_hash text unique,
  created_at timestamptz not null default now()
);

create index if not exists idx_raw_events_source on raw_events(source);
create index if not exists idx_raw_events_event_date on raw_events(event_date);
create index if not exists idx_raw_events_issuer on raw_events(issuer_name);

create table if not exists normalized_events (
  id uuid primary key default gen_random_uuid(),
  raw_event_id uuid references raw_events(id) on delete cascade,
  issuer_name_normalized text not null,
  person_name_normalized text,
  event_date date,
  source text not null,
  event_class text,
  event_subclass text,
  transaction_direction text,
  price numeric,
  quantity numeric,
  currency text,
  estimated_value numeric,
  role_title text,
  detail_status text default 'pending',
  source_url text,
  raw_summary text,
  created_at timestamptz not null default now()
);

create index if not exists idx_normalized_events_issuer on normalized_events(issuer_name_normalized);
create index if not exists idx_normalized_events_event_date on normalized_events(event_date);
create index if not exists idx_normalized_events_source on normalized_events(source);

create table if not exists scored_events (
  id uuid primary key default gen_random_uuid(),
  normalized_event_id uuid references normalized_events(id) on delete cascade,
  rule_score integer not null default 0,
  liquidity_score integer not null default 0,
  confidence_score integer not null default 0,
  keep_flag boolean not null default false,
  rejection_reason text,
  explanation text,
  ai_label text,
  ai_confidence text,
  created_at timestamptz not null default now(),
  unique(normalized_event_id)
);

create index if not exists idx_scored_events_keep_flag on scored_events(keep_flag);
create index if not exists idx_scored_events_rule_score on scored_events(rule_score);

create table if not exists issuer_opportunities (
  id uuid primary key default gen_random_uuid(),
  issuer_name text not null,
  first_seen date,
  last_seen date,
  event_count integer not null default 0,
  total_score integer not null default 0,
  top_signal_type text,
  best_person_name text,
  best_angle text,
  supporting_evidence jsonb not null default '[]'::jsonb,
  status text not null default 'new',
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  unique(issuer_name)
);

create index if not exists idx_issuer_opportunities_total_score on issuer_opportunities(total_score);
create index if not exists idx_issuer_opportunities_status on issuer_opportunities(status);

create table if not exists demand_targets (
  id uuid primary key default gen_random_uuid(),
  issuer_opportunity_id uuid references issuer_opportunities(id) on delete cascade,
  issuer_name text not null,
  person_name text,
  role_title text,
  linkedin_url text,
  email text,
  contact_confidence text,
  route_type text,
  outreach_status text not null default 'not_started',
  notes text,
  created_at timestamptz not null default now()
);

create index if not exists idx_demand_targets_issuer on demand_targets(issuer_name);
create index if not exists idx_demand_targets_outreach_status on demand_targets(outreach_status);

create or replace function set_updated_at()
returns trigger as $$
begin
  new.updated_at = now();
  return new;
end;
$$ language plpgsql;

drop trigger if exists trg_issuer_opportunities_updated_at on issuer_opportunities;
create trigger trg_issuer_opportunities_updated_at
before update on issuer_opportunities
for each row
execute function set_updated_at();

# GTM Semantic Views — AI & Developer Guide

## Overview

This directory contains two Snowflake Semantic Views for Workiva's Go-To-Market (GTM) analytics, built on the **dimensional model** (fact + SCD Type 2 dimension tables) rather than the flattened OBT (One Big Table) approach.

| Semantic View | Fact Table | Type | File |
|---|---|---|---|
| **SV_GTM_PIPELINE** | `fct_pipeline` | Daily snapshot (semi-additive) | `sv_gtm_pipeline.sql` |
| **SV_GTM_BOOKINGS** | `fct_bookings` | Transactional (additive) | `sv_gtm_bookings.sql` |

---

## Architecture

### Dimensional Model (Star Schema)

Both views follow a star schema pattern where the central fact table joins to multiple SCD Type 2 dimension tables via **temporal range joins**. This means dimension attributes are resolved to the version that was active at the time of the business event (snapshot date for pipeline, booking date for bookings).

```
                        dim_users (owner)
                             |
                        dim_users (creator)
                             |
dim_accounts ---- fct_pipeline/fct_bookings ---- dim_opportunity_alignments
                             |
                   dim_opportunities
                             |
              dim_opportunity_solution_classifications
                             |
                   dim_contacts
                             |
              dim_opportunity_partners
                             |
         dim_opportunity_line_item_summary
                             |
         dim_opportunity_assist_summary
```

### SCD Type 2 Temporal Joins

All dimension tables use Slowly Changing Dimension Type 2 with these columns:
- `version_start_at` — Start of the version's validity period (inclusive)
- `version_end_at` — End of the version's validity period (exclusive, NULL = current)
- `is_latest` — TRUE for the currently active version

The semantic views use Snowflake's `CONSTRAINT DISTINCT RANGE BETWEEN ... EXCLUSIVE` syntax to declare non-overlapping validity ranges, then join facts to dimensions using temporal range references:

```sql
-- Pipeline: uses snapshot_end_of_day_timestamp as temporal key
pipeline(opportunity_id, snapshot_end_of_day_timestamp)
  REFERENCES opportunities(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE)

-- Bookings: uses booking_or_closed_date cast to TIMESTAMP_NTZ
bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
  REFERENCES opportunities(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE)
```

This means queries automatically get the **historically correct** dimension values without any manual join logic.

---

## Deployment

### Prerequisites

Replace the `{{DATABASE}}` and `{{SCHEMA}}` placeholders in both SQL files with your actual Snowflake database and schema names before executing.

### Execution Order

1. Deploy `sv_gtm_pipeline.sql` first (no cross-view dependencies)
2. Deploy `sv_gtm_bookings.sql` second (no cross-view dependencies)

Both views are independent — they can be deployed in any order or in parallel.

```sql
-- Example: Replace placeholders and execute
-- Option 1: Find-and-replace in your SQL editor
-- Option 2: Use a deployment script
SET DATABASE_NAME = 'PROD_DB';
SET SCHEMA_NAME = 'ANALYTICS';
-- Then execute each file after replacing {{DATABASE}} and {{SCHEMA}}
```

### Permissions

Grant access to end users:
```sql
GRANT SELECT ON SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.SV_GTM_PIPELINE TO ROLE analyst_role;
GRANT SELECT ON SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.SV_GTM_BOOKINGS TO ROLE analyst_role;
-- Note: Users with semantic view access don't need separate table grants (owner's rights)
```

---

## Pipeline Semantic View (SV_GTM_PIPELINE)

### Key Concepts

**Snapshot-based fact table**: Each row in `fct_pipeline` represents one opportunity-solution combination on a specific `snapshot_date`. The same opportunity appears across many snapshot dates, which means naive aggregations (like `SUM`) across dates will double-count.

**Semi-additive metrics**: Use `NON ADDITIVE BY (pipeline.snapshot_date_dim)` to automatically resolve to the latest snapshot and prevent double-counting. Always prefer these for "current state" questions:
- `latest_total_pipeline` — Current total pipeline
- `latest_qualified_pipeline` — Current qualified pipeline
- `latest_opportunity_count` — Current deal count
- `latest_account_count` — Current account count

**Standard metrics** (additive across dates): Use for trend analysis over time:
- `total_pipeline` — Sum across all selected snapshots (use with snapshot_date filter)
- `qualified_pipeline` / `unqualified_pipeline`
- `opportunity_count` / `account_count`

### Tables Joined

| Alias | Physical Table | Join Key | Relationship |
|---|---|---|---|
| pipeline | FCT_PIPELINE | (primary) | Fact table |
| opportunities | DIM_OPPORTUNITIES | opportunity_id + temporal | 1:many via snapshot |
| accounts | DIM_ACCOUNTS | customer_account_id + temporal | 1:many via snapshot |
| opp_owner | DIM_USERS | opportunity_owner_user_id + temporal | Multi-path user |
| opp_creator | DIM_USERS | opportunity_created_by_user_id + temporal | Multi-path user |
| owner_manager | DIM_USERS | manager_user_id (chained from owner) | Chained relationship |
| alignments | DIM_OPPORTUNITY_ALIGNMENTS | opportunity_id + temporal | 1:1 via snapshot |
| solution_classifications | DIM_OPPORTUNITY_SOLUTION_CLASSIFICATIONS | opp_solution_classification_key + temporal | 1:many via snapshot |
| contacts | DIM_CONTACTS | first_associated_contact_id + temporal | 1:many via snapshot |
| partners | DIM_OPPORTUNITY_PARTNERS | opportunity_id + temporal | 1:1 via snapshot |
| line_items | DIM_OPPORTUNITY_LINE_ITEM_SUMMARY | opportunity_id + temporal | 1:1 via snapshot |
| assists | DIM_OPPORTUNITY_ASSIST_SUMMARY | opportunity_id + temporal | 1:1 via snapshot |

### Key Snapshot Date Filters

| Dimension | When to Use |
|---|---|
| `is_current_day_snapshot = TRUE` | Latest available snapshot |
| `is_fifth_business_day_of_quarter_snapshot = TRUE` | Standard quarterly reporting date |
| `is_fifth_business_day_of_month_snapshot = TRUE` | Standard monthly reporting date |
| `is_one_year_ago_snapshot = TRUE` | Same day last year (for YoY comparison) |
| `is_quarterly_snapshot = TRUE` | Key dates for quarterly reports |
| `is_monthly_snapshot = TRUE` | Key dates for monthly reports |

### Period-over-Period Metrics

| Metric | Comparison | LAG Offset |
|---|---|---|
| `total_pipeline_yoy` | Same day last year | 365 days |
| `total_pipeline_qoq` | ~One quarter ago | 91 days |
| `total_pipeline_mom` | ~One month ago | 30 days |
| `total_pipeline_7d_avg` | Rolling 7-day average | RANGE 6 days |
| `total_pipeline_30d_avg` | Rolling 30-day average | RANGE 29 days |
| `total_pipeline_prev_snapshot` | Previous day | 1 day |
| `pipeline_yoy_pct_change` | YoY % change | Derived |
| `pipeline_qoq_pct_change` | QoQ % change | Derived |
| `pipeline_mom_pct_change` | MoM % change | Derived |

**Important**: Window metrics require their time dimensions in any query. For example, `total_pipeline_yoy` requires `snapshot_date_dim` and `snapshot_year_dim`.

### Multi-Path Metrics

When grouping by dimensions from a specific table that has multiple join paths, use the appropriate `USING` metric:

| Grouping By | Use Metric |
|---|---|
| Account dimensions (account_name, account_region, etc.) | `latest_total_pipeline_by_account` |
| Owner dimensions (opportunity_owner_name, etc.) | `latest_total_pipeline_by_owner` |
| Alignment dimensions (opportunity_market_segment, etc.) | `latest_total_pipeline_by_alignment` |

---

## Bookings Semantic View (SV_GTM_BOOKINGS)

### Key Concepts

**Transactional fact table**: Each row in `fct_bookings` represents a single booking event (one opportunity-solution at close). Unlike pipeline, there's no snapshot duplication, so standard aggregation metrics work without semi-additive treatment.

**Booking Classifications** (via `reporting_classification` dimension):
| Classification | Meaning | Common Abbreviation |
|---|---|---|
| NL | New Logo — first-time customer | New Logo |
| NS | New Solution — existing customer, new product | New Solution |
| Reno | Renegotiation — contract renegotiation | Renegotiation |
| PI-Comm Ops | Price Increase | PI |
| Churn | Customer churn (typically negative amounts) | Churn |

**Metric Booking Definition**: In the original OBT, `is_metric_booking` means `(is_closed_won OR is_closed_pending) AND NOT churned`. In the dimensional model, replicate this with:
```sql
WHERE (is_closed_won = TRUE OR is_closed_pending = TRUE)
  AND reporting_classification != 'Churn'
```

**Non-PI Bookings**: A frequently requested metric excluding price increases:
```sql
WHERE (is_closed_won = TRUE OR is_closed_pending = TRUE)
  AND reporting_classification NOT IN ('PI-Comm Ops', 'Churn')
```

### Tables Joined

Same as pipeline, plus:
| Alias | Physical Table | Join Key |
|---|---|---|
| contracts | DIM_CONTRACTS | first_contract_id + temporal |

### Period-over-Period Metrics

| Metric | Comparison | LAG Offset |
|---|---|---|
| `total_bookings_yoy` | Same day last year | 365 days |
| `total_bookings_qoq` | ~One quarter ago | 91 days |
| `total_bookings_mom` | ~One month ago | 30 days |
| `total_bookings_7d_avg` | Rolling 7-day average | RANGE 6 days |
| `total_bookings_30d_avg` | Rolling 30-day average | RANGE 29 days |
| `bookings_yoy_pct_change` | YoY % change | Derived |
| `bookings_qoq_pct_change` | QoQ % change | Derived |
| `bookings_mom_pct_change` | MoM % change | Derived |

---

## Common Query Patterns for Cortex Agents

### Pipeline Queries

```sql
-- Current total pipeline
SELECT * FROM SEMANTIC_VIEW(SV_GTM_PIPELINE
  METRICS pipeline.latest_total_pipeline);

-- Pipeline by segment for current snapshot
SELECT * FROM SEMANTIC_VIEW(SV_GTM_PIPELINE
  DIMENSIONS alignments.opportunity_market_segment_dim
  METRICS pipeline.latest_total_pipeline_by_alignment);

-- YoY pipeline comparison by date
SELECT * FROM SEMANTIC_VIEW(SV_GTM_PIPELINE
  DIMENSIONS pipeline.snapshot_date_dim, pipeline.snapshot_year_dim
  METRICS pipeline.total_pipeline, pipeline.total_pipeline_yoy, pipeline_yoy_pct_change);

-- Qualified pipeline at 5th business day of each quarter
SELECT * FROM SEMANTIC_VIEW(SV_GTM_PIPELINE
  DIMENSIONS pipeline.snapshot_date_dim, pipeline.snapshot_year_quarter_dim
  METRICS pipeline.qualified_pipeline)
WHERE is_fifth_business_day_of_quarter_snapshot = TRUE AND is_open = TRUE;
```

### Bookings Queries

```sql
-- Total NL bookings this quarter
SELECT * FROM SEMANTIC_VIEW(SV_GTM_BOOKINGS
  DIMENSIONS bookings.booking_quarter_dim
  METRICS bookings.total_bookings)
WHERE reporting_classification = 'NL'
  AND (is_closed_won = TRUE OR is_closed_pending = TRUE)
  AND booking_quarter_dim = CONCAT('Q', QUARTER(CURRENT_DATE), ' ', YEAR(CURRENT_DATE));

-- Bookings by account region (Non-PI)
SELECT * FROM SEMANTIC_VIEW(SV_GTM_BOOKINGS
  DIMENSIONS accounts.account_region_dim
  METRICS bookings.total_bookings_by_account, bookings.booking_count_by_account)
WHERE reporting_classification NOT IN ('PI-Comm Ops', 'Churn')
  AND (is_closed_won = TRUE OR is_closed_pending = TRUE);

-- YoY bookings trend
SELECT * FROM SEMANTIC_VIEW(SV_GTM_BOOKINGS
  DIMENSIONS bookings.booking_date_dim, bookings.booking_year_dim
  METRICS bookings.total_bookings, bookings.total_bookings_yoy, bookings_yoy_pct_change);
```

---

## Differences from OBT-Based Semantic Views

| Aspect | OBT-Based (sv_bookings, sv_pipeline) | Dimensional (sv_gtm_bookings, sv_gtm_pipeline) |
|---|---|---|
| Source tables | Single OBT table | Fact + 11-12 dimension tables |
| Join handling | Pre-joined in dbt | Temporal range joins in semantic view |
| Historical accuracy | Fixed at OBT build time | Point-in-time via SCD2 range joins |
| Flexibility | Limited to OBT columns | Full access to all dimension columns |
| Metric flags | dbt macros (is_metric_booking, etc.) | Replicated via CASE expressions and dimension filters |
| Format | dbt YAML (materialized='semantic_view') | Snowflake DDL (CREATE SEMANTIC VIEW) |
| Semi-additive | Not supported | NON ADDITIVE BY for snapshot metrics |
| Multi-path | Not needed (single table) | USING clause for account/owner/alignment paths |
| Period-over-period | Not included | YoY/QoQ/MoM via LAG window metrics |
| AI optimization | Basic descriptions | Comprehensive AI_SQL_GENERATION and AI_QUESTION_CATEGORIZATION |

---

## Maintenance Notes

1. **Adding new dimensions**: Add the column reference in the DIMENSIONS section with a COMMENT and optional SYNONYMS. If the column comes from a new table, add it to TABLES and RELATIONSHIPS first.

2. **Adding new metrics**: Add to the METRICS section. Use `NON ADDITIVE BY` for pipeline snapshot metrics. Use `USING` when the metric needs a specific join path.

3. **Updating AI hints**: Modify the `AI_SQL_GENERATION` and `AI_QUESTION_CATEGORIZATION` strings. Keep SQL generation rules focused on HOW to write queries; keep question categorization focused on WHETHER to answer.

4. **Schema changes**: If a dimension table adds new SCD2 columns, they're automatically available — just add the dimension/fact reference. If column names change, update the references.

5. **Placeholder replacement**: Before deployment, replace all `{{DATABASE}}` and `{{SCHEMA}}` with actual values. Consider using a CI/CD pipeline or dbt macro for this.

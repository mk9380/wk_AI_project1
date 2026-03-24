CREATE OR REPLACE SEMANTIC VIEW WORKIVA_PIPELINE_ANALYTICS

  TABLES (
    pipeline AS {{DATABASE}}.{{SCHEMA}}.FCT_PIPELINE
      PRIMARY KEY (pipeline_id),

    opportunities AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITIES
      PRIMARY KEY (opportunity_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT opp_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    accounts AS {{DATABASE}}.{{SCHEMA}}.DIM_ACCOUNTS
      PRIMARY KEY (account_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT acct_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    contacts AS {{DATABASE}}.{{SCHEMA}}.DIM_CONTACTS
      PRIMARY KEY (contact_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT contact_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    users AS {{DATABASE}}.{{SCHEMA}}.DIM_USERS
      PRIMARY KEY (user_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT user_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    alignments AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_ALIGNMENTS
      PRIMARY KEY (opportunity_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT align_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    partners AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_PARTNERS
      PRIMARY KEY (opportunity_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT partner_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    solution_classifications AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_SOLUTION_CLASSIFICATIONS
      PRIMARY KEY (opportunity_solution_classification_key)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT solclass_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    contact_roles AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_CONTACT_ROLES
      PRIMARY KEY (opportunity_contact_role_id)
      UNIQUE (valid_from, valid_to)
      CONSTRAINT crole_range DISTINCT RANGE BETWEEN valid_from AND valid_to EXCLUSIVE,

    calendar_dates AS {{DATABASE}}.{{SCHEMA}}.DIM_CALENDAR_DATES
      PRIMARY KEY (date_id),

    conversion_rates AS {{DATABASE}}.{{SCHEMA}}.DIM_CONVERSION_RATES
      PRIMARY KEY (currency_iso_code)
      UNIQUE (effective_start_date, effective_end_date)
      CONSTRAINT fx_range DISTINCT RANGE BETWEEN effective_start_date AND effective_end_date EXCLUSIVE
  )

  RELATIONSHIPS (
    pipeline_to_opportunities AS
      pipeline(opportunity_id, snapshot_timestamp) REFERENCES opportunities(opportunity_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_accounts AS
      pipeline(account_id, snapshot_timestamp) REFERENCES accounts(account_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_solution_classifications AS
      pipeline(opportunity_solution_classification_key, snapshot_timestamp) REFERENCES solution_classifications(opportunity_solution_classification_key, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_owner AS
      pipeline(opportunity_owner_user_id, snapshot_timestamp) REFERENCES users(user_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_creator AS
      pipeline(opportunity_creator_user_id, snapshot_timestamp) REFERENCES users(user_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_contact AS
      pipeline(first_associated_contact_id, snapshot_timestamp) REFERENCES contacts(contact_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    pipeline_to_conversion_rates AS
      pipeline(currency_iso_code, snapshot_date) REFERENCES conversion_rates(currency_iso_code, BETWEEN effective_start_date AND effective_end_date EXCLUSIVE),

    pipeline_to_calendar AS
      pipeline(snapshot_date) REFERENCES calendar_dates(date_id),

    opportunities_to_alignments AS
      opportunities(opportunity_id, valid_from) REFERENCES alignments(opportunity_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    opportunities_to_partners AS
      opportunities(opportunity_id, valid_from) REFERENCES partners(opportunity_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    contacts_to_accounts AS
      contacts(account_id, valid_from) REFERENCES accounts(account_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    contact_roles_to_opportunities AS
      contact_roles(opportunity_id, valid_from) REFERENCES opportunities(opportunity_id, BETWEEN valid_from AND valid_to EXCLUSIVE),

    contact_roles_to_contacts AS
      contact_roles(contact_id, valid_from) REFERENCES contacts(contact_id, BETWEEN valid_from AND valid_to EXCLUSIVE)
  )

  -- =========================================================================
  -- FACTS
  -- =========================================================================
  FACTS (
    -- Pipeline core
    PRIVATE pipeline.snapshot_timestamp AS snapshot_date::TIMESTAMP_NTZ
      COMMENT = 'Snapshot date cast to timestamp for range join compatibility',

    pipeline.pipeline_amount_usd AS pipeline_a_s_usd
      COMMENT = 'Pipeline amount in USD',

    pipeline.snapshot_date_value AS snapshot_date
      COMMENT = 'Date of the pipeline snapshot',

    pipeline.is_opportunity_with_solution_total AS is_opportunity_with_solution_total
      COMMENT = 'Whether the opportunity has a solution total',

    -- Stage duration & movement (computed during snapshot ETL)
    pipeline.days_in_current_stage AS days_in_current_stage
      COMMENT = 'Number of days the opportunity has been in its current stage as of this snapshot',

    pipeline.previous_snapshot_stage AS previous_snapshot_stage
      COMMENT = 'The opportunity stage on the prior snapshot date — NULL if first snapshot',

    pipeline.is_stage_change AS is_stage_change
      COMMENT = 'TRUE if the stage changed between this snapshot and the previous one',

    -- Pipeline waterfall classification (computed during snapshot ETL)
    pipeline.pipeline_category AS pipeline_category
      COMMENT = 'Waterfall category: Beginning, New, Expansion, Contraction, Closed Won, Closed Lost, Pushed',

    pipeline.quarter_start_amount AS quarter_start_amount
      COMMENT = 'Pipeline amount at the first snapshot of the current fiscal quarter — NULL if opp was not in pipeline at quarter start',

    pipeline.is_new_in_quarter AS is_new_in_quarter
      COMMENT = 'TRUE if this opportunity first appeared in the pipeline during the current quarter',

    -- Opportunity attributes
    opportunities.opportunity_name AS opportunity_name
      COMMENT = 'Name of the opportunity',

    opportunities.opportunity_type AS opportunity_type
      COMMENT = 'Type of opportunity (e.g. New Business, Renewal, Expansion)',

    opportunities.opportunity_stage AS opportunity_stage
      COMMENT = 'Current stage of the opportunity',

    opportunities.record_type_name AS record_type_name
      COMMENT = 'Salesforce record type',

    -- Account attributes
    accounts.account_name AS account_name
      COMMENT = 'Name of the account',

    accounts.account_type AS account_type
      COMMENT = 'Type of the account',

    accounts.account_region AS account_region
      COMMENT = 'Region of the account',

    accounts.account_owner_role AS account_owner_role
      COMMENT = 'Role of the account owner',

    accounts.is_test_or_internal_account AS is_test_or_internal_account
      COMMENT = 'Flag for test or internal accounts',

    -- Contact & user attributes
    contacts.contact_title AS contact_title
      COMMENT = 'Title of the contact',

    contacts.contact_first_name AS contact_first_name
      COMMENT = 'First name of the contact',

    contacts.contact_last_name AS contact_last_name
      COMMENT = 'Last name of the contact',

    users.user_name AS user_name
      COMMENT = 'Name of the user',

    -- Alignment attributes
    alignments.segment AS segment
      COMMENT = 'Sales segment alignment',

    alignments.region AS region
      COMMENT = 'Sales region alignment',

    alignments.sub_region AS sub_region
      COMMENT = 'Sales sub-region alignment',

    -- Solution attributes
    solution_classifications.booking_category AS booking_category
      COMMENT = 'Booking category for the solution',

    solution_classifications.solution_category AS solution_category
      COMMENT = 'Solution category',

    solution_classifications.solution_picklist AS solution_picklist
      COMMENT = 'Solution picklist value',

    solution_classifications.sku_grouping AS sku_grouping
      COMMENT = 'SKU grouping',

    solution_classifications.solution_group AS solution_group
      COMMENT = 'Solution group',

    -- Other
    contact_roles.contact_role AS contact_role
      COMMENT = 'Role of the contact on the opportunity',

    conversion_rates.conversion_rate AS conversion_rate
      COMMENT = 'Currency conversion rate to USD',

    calendar_dates.bus_date AS bus_date
      COMMENT = 'Business date from calendar'
  )

  -- =========================================================================
  -- DIMENSIONS
  -- =========================================================================
  DIMENSIONS (
    -- Time dimensions
    pipeline.snapshot_date_dim AS snapshot_date
      WITH SYNONYMS = ('pipeline date', 'snapshot')
      COMMENT = 'Date of the pipeline snapshot',

    pipeline.snapshot_year_dim AS YEAR(snapshot_date)
      WITH SYNONYMS = ('year', 'fiscal year')
      COMMENT = 'Year of the pipeline snapshot',

    pipeline.snapshot_quarter_dim AS CONCAT('Q', QUARTER(snapshot_date), ' ', YEAR(snapshot_date))
      WITH SYNONYMS = ('quarter', 'fiscal quarter')
      COMMENT = 'Quarter of the pipeline snapshot (e.g. Q1 2024)',

    pipeline.snapshot_month_dim AS TO_VARCHAR(snapshot_date, 'YYYY-MM')
      WITH SYNONYMS = ('month', 'snapshot month')
      COMMENT = 'Month of the pipeline snapshot (e.g. 2024-01)',

    pipeline.snapshot_week_dim AS TO_VARCHAR(DATE_TRUNC('WEEK', snapshot_date), 'YYYY-MM-DD')
      WITH SYNONYMS = ('week', 'snapshot week')
      COMMENT = 'Week starting date of the pipeline snapshot',

    -- Pipeline waterfall dimension
    pipeline.pipeline_category_dim AS pipeline_category
      WITH SYNONYMS = ('waterfall category', 'pipeline movement type')
      COMMENT = 'Pipeline waterfall category: Beginning, New, Expansion, Contraction, Closed Won, Closed Lost, Pushed',

    -- Opportunity dimensions
    opportunities.opportunity_name_dim AS opportunity_name
      WITH SYNONYMS = ('opp name', 'deal name')
      COMMENT = 'Name of the opportunity',

    opportunities.opportunity_type_dim AS opportunity_type
      WITH SYNONYMS = ('opp type', 'deal type')
      COMMENT = 'Type of opportunity',

    opportunities.opportunity_stage_dim AS opportunity_stage
      WITH SYNONYMS = ('opp stage', 'deal stage', 'sales stage')
      COMMENT = 'Current sales stage of the opportunity',

    opportunities.record_type_name_dim AS record_type_name
      WITH SYNONYMS = ('record type')
      COMMENT = 'Salesforce record type name',

    -- Account dimensions
    accounts.account_name_dim AS account_name
      WITH SYNONYMS = ('company name', 'customer name')
      COMMENT = 'Name of the account',

    accounts.account_type_dim AS account_type
      COMMENT = 'Type classification of the account',

    accounts.account_region_dim AS account_region
      WITH SYNONYMS = ('account geo')
      COMMENT = 'Geographic region of the account',

    accounts.account_owner_role_dim AS account_owner_role
      COMMENT = 'Role of the account owner',

    accounts.is_test_or_internal_dim AS is_test_or_internal_account
      COMMENT = 'Whether this is a test or internal account',

    -- Contact & user dimensions
    contacts.contact_title_dim AS contact_title
      COMMENT = 'Job title of the primary contact',

    users.user_name_dim AS user_name
      WITH SYNONYMS = ('rep name', 'owner name', 'sales rep')
      COMMENT = 'Name of the opportunity owner or creator',

    -- Alignment dimensions
    alignments.segment_dim AS segment
      WITH SYNONYMS = ('sales segment', 'market segment')
      COMMENT = 'Sales segment alignment',

    alignments.region_dim AS region
      WITH SYNONYMS = ('sales region', 'geo')
      COMMENT = 'Sales region alignment',

    alignments.sub_region_dim AS sub_region
      WITH SYNONYMS = ('sales sub-region', 'sub region', 'territory')
      COMMENT = 'Sales sub-region alignment',

    -- Solution dimensions
    solution_classifications.booking_category_dim AS booking_category
      WITH SYNONYMS = ('booking type')
      COMMENT = 'Booking category for the solution',

    solution_classifications.solution_category_dim AS solution_category
      WITH SYNONYMS = ('product category')
      COMMENT = 'Solution category',

    solution_classifications.solution_picklist_dim AS solution_picklist
      COMMENT = 'Solution picklist value',

    solution_classifications.sku_grouping_dim AS sku_grouping
      WITH SYNONYMS = ('product grouping', 'sku group')
      COMMENT = 'SKU grouping',

    solution_classifications.solution_group_dim AS solution_group
      WITH SYNONYMS = ('product group')
      COMMENT = 'Solution group',

    -- Other dimensions
    contact_roles.contact_role_dim AS contact_role
      COMMENT = 'Role of the contact on the opportunity',

    calendar_dates.bus_date_dim AS bus_date
      WITH SYNONYMS = ('business date', 'calendar date')
      COMMENT = 'Business date from calendar dimension'
  )

  -- =========================================================================
  -- METRICS
  -- =========================================================================
  METRICS (
    -- ===== Base aggregation metrics =====

    pipeline.total_pipeline_amount AS SUM(pipeline_a_s_usd)
      WITH SYNONYMS = ('total pipeline', 'pipeline value', 'total pipe')
      COMMENT = 'Total pipeline amount in USD',

    pipeline.avg_pipeline_amount AS AVG(pipeline_a_s_usd)
      WITH SYNONYMS = ('average deal size', 'avg deal value')
      COMMENT = 'Average pipeline amount per record in USD',

    pipeline.max_pipeline_amount AS MAX(pipeline_a_s_usd)
      COMMENT = 'Largest single pipeline amount in USD',

    pipeline.min_pipeline_amount AS MIN(pipeline_a_s_usd)
      COMMENT = 'Smallest single pipeline amount in USD',

    pipeline.pipeline_record_count AS COUNT(pipeline_id)
      WITH SYNONYMS = ('number of pipeline records', 'pipeline count')
      COMMENT = 'Count of pipeline snapshot records',

    pipeline.opportunity_count AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('number of opportunities', 'deal count', 'opp count')
      COMMENT = 'Count of distinct opportunities in the pipeline',

    pipeline.account_count AS COUNT(DISTINCT account_id)
      WITH SYNONYMS = ('number of accounts', 'customer count')
      COMMENT = 'Count of distinct accounts in the pipeline',

    pipeline.opportunities_with_solution_total AS SUM(
      CASE WHEN is_opportunity_with_solution_total = TRUE THEN 1 ELSE 0 END
    )
      COMMENT = 'Count of opportunities that have a solution total',

    -- ===== Stage duration metrics =====

    pipeline.avg_days_in_stage AS AVG(days_in_current_stage)
      WITH SYNONYMS = ('average stage duration', 'avg time in stage')
      COMMENT = 'Average number of days opportunities spend in their current stage',

    pipeline.max_days_in_stage AS MAX(days_in_current_stage)
      COMMENT = 'Maximum days any opportunity has spent in its current stage',

    pipeline.stage_change_count AS SUM(CASE WHEN is_stage_change = TRUE THEN 1 ELSE 0 END)
      WITH SYNONYMS = ('number of stage changes', 'stage transitions')
      COMMENT = 'Count of stage changes observed in the snapshot period',

    -- ===== Pipeline waterfall metrics =====

    pipeline.beginning_pipeline AS SUM(CASE WHEN pipeline_category = 'Beginning' THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('starting pipeline', 'opening pipeline')
      COMMENT = 'Pipeline amount carried over from the start of the period',

    pipeline.new_pipeline AS SUM(CASE WHEN pipeline_category = 'New' THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('new pipe', 'newly created pipeline')
      COMMENT = 'Pipeline amount from opportunities newly created in the period',

    pipeline.closed_won_pipeline AS SUM(CASE WHEN pipeline_category = 'Closed Won' THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('won pipeline', 'bookings')
      COMMENT = 'Pipeline amount that closed won in the period',

    pipeline.closed_lost_pipeline AS SUM(CASE WHEN pipeline_category = 'Closed Lost' THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('lost pipeline', 'leaked pipeline')
      COMMENT = 'Pipeline amount that closed lost (leaked) in the period',

    pipeline.pushed_pipeline AS SUM(CASE WHEN pipeline_category = 'Pushed' THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('slipped pipeline', 'pushed to next quarter')
      COMMENT = 'Pipeline amount pushed out to a future period',

    pipeline.new_in_quarter_count AS SUM(CASE WHEN is_new_in_quarter = TRUE THEN 1 ELSE 0 END)
      WITH SYNONYMS = ('new deals this quarter', 'new opps in quarter')
      COMMENT = 'Count of opportunities newly created in the current quarter',

    -- ===== Conversion metrics =====

    pipeline.closed_won_count AS COUNT(DISTINCT CASE WHEN opportunities.opportunity_stage = 'Closed Won' THEN opportunity_id END)
      WITH SYNONYMS = ('wins', 'won deals')
      COMMENT = 'Count of distinct opportunities that closed won',

    pipeline.closed_lost_count AS COUNT(DISTINCT CASE WHEN opportunities.opportunity_stage = 'Closed Lost' THEN opportunity_id END)
      WITH SYNONYMS = ('losses', 'lost deals')
      COMMENT = 'Count of distinct opportunities that closed lost',

    pipeline.commit_count AS COUNT(DISTINCT CASE WHEN opportunities.opportunity_stage IN ('Negotiation', 'Closed Won') THEN opportunity_id END)
      WITH SYNONYMS = ('commit deals', 'committed pipeline count')
      COMMENT = 'Count of distinct opportunities in Commit stage (Negotiation or Closed Won)',

    pipeline.commit_pipeline AS SUM(CASE WHEN opportunities.opportunity_stage IN ('Negotiation', 'Closed Won') THEN pipeline_a_s_usd ELSE 0 END)
      WITH SYNONYMS = ('committed pipeline amount', 'commit value')
      COMMENT = 'Pipeline amount in Commit stage (Negotiation or Closed Won)',

    -- ===== Semi-additive metrics (snapshot-safe) =====

    pipeline.latest_pipeline_amount
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(pipeline_a_s_usd)
      WITH SYNONYMS = ('current pipeline', 'latest pipe')
      COMMENT = 'Pipeline amount from the latest snapshot — avoids double-counting across snapshots',

    pipeline.latest_opportunity_count
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('current opportunity count', 'current deal count')
      COMMENT = 'Distinct opportunity count from the latest snapshot',

    pipeline.latest_account_count
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT account_id)
      WITH SYNONYMS = ('current account count', 'active accounts')
      COMMENT = 'Distinct account count from the latest snapshot — use for current-state account counts',

    -- ===== Multi-path metrics (account dimensions via pipeline_to_accounts) =====

    pipeline.latest_pipeline_amount_by_account
      USING (pipeline_to_accounts)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(pipeline_a_s_usd)
      WITH SYNONYMS = ('pipeline by account', 'account pipeline')
      COMMENT = 'Latest pipeline amount resolved via accounts — use when grouping by account dimensions',

    pipeline.latest_opportunity_count_by_account
      USING (pipeline_to_accounts)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      COMMENT = 'Latest opportunity count resolved via accounts — use when grouping by account dimensions',

    pipeline.latest_account_count_by_account
      USING (pipeline_to_accounts)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT account_id)
      COMMENT = 'Latest account count resolved via accounts — use when grouping by account dimensions',

    pipeline.total_pipeline_amount_by_account
      USING (pipeline_to_accounts)
      AS SUM(pipeline_a_s_usd)
      COMMENT = 'Total pipeline amount resolved via accounts — use when grouping by account dimensions',

    pipeline.account_count_by_account
      USING (pipeline_to_accounts)
      AS COUNT(DISTINCT account_id)
      COMMENT = 'Account count resolved via accounts',

    -- ===== Multi-path metrics (user dimensions via pipeline_to_owner) =====

    pipeline.latest_pipeline_amount_by_owner
      USING (pipeline_to_owner)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(pipeline_a_s_usd)
      WITH SYNONYMS = ('pipeline by rep', 'rep pipeline')
      COMMENT = 'Latest pipeline amount resolved via opportunity owner — use when grouping by user dimensions',

    pipeline.latest_opportunity_count_by_owner
      USING (pipeline_to_owner)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('opportunities by rep', 'rep opportunity count')
      COMMENT = 'Latest opportunity count resolved via opportunity owner — use when grouping by user dimensions',

    -- ===== Window metrics (period-over-period) =====

    pipeline.pipeline_amount_prev_snapshot AS LAG(pipeline.total_pipeline_amount, 1)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('previous pipeline amount', 'prior snapshot pipeline')
      COMMENT = 'Total pipeline amount from the previous snapshot date',

    pipeline.opportunity_count_prev_snapshot AS LAG(pipeline.opportunity_count, 1)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim ORDER BY pipeline.snapshot_date_dim)
      COMMENT = 'Opportunity count from the previous snapshot date',

    pipeline.pipeline_amount_7d_avg AS AVG(pipeline.total_pipeline_amount)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim ORDER BY pipeline.snapshot_date_dim
        RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)
      WITH SYNONYMS = ('7-day average pipeline', 'rolling weekly pipeline')
      COMMENT = 'Rolling 7-day average of total pipeline amount',

    pipeline.pipeline_amount_yoy AS LAG(pipeline.total_pipeline_amount, 52)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('pipeline same week last year', 'prior year pipeline')
      COMMENT = 'Total pipeline amount from the same week one year ago (52 snapshots back)',

    pipeline.avg_days_in_stage_prev_quarter AS LAG(pipeline.avg_days_in_stage, 13)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim ORDER BY pipeline.snapshot_date_dim)
      COMMENT = 'Average days in stage from approximately one quarter ago (13 weeks back)',

    -- ===== Derived metrics =====

    avg_pipeline_per_account AS
      DIV0(pipeline.total_pipeline_amount, pipeline.account_count)
      WITH SYNONYMS = ('pipeline per account', 'account average pipeline')
      COMMENT = 'Average pipeline amount per account',

    avg_pipeline_per_opportunity AS
      DIV0(pipeline.total_pipeline_amount, pipeline.opportunity_count)
      WITH SYNONYMS = ('pipeline per opportunity', 'average deal size')
      COMMENT = 'Average pipeline amount per opportunity',

    solution_total_rate AS
      DIV0(pipeline.opportunities_with_solution_total, pipeline.opportunity_count) * 100
      WITH SYNONYMS = ('solution coverage rate', 'pct with solution total')
      COMMENT = 'Percentage of opportunities that have a solution total',

    commit_to_close_rate AS
      DIV0(pipeline.closed_won_count, pipeline.commit_count) * 100
      WITH SYNONYMS = ('commit conversion rate', 'win rate from commit')
      COMMENT = 'Percentage of Commit-stage deals that reached Closed Won',

    win_rate AS
      DIV0(pipeline.closed_won_count, pipeline.closed_won_count + pipeline.closed_lost_count) * 100
      WITH SYNONYMS = ('overall win rate', 'close rate')
      COMMENT = 'Win rate: Closed Won / (Closed Won + Closed Lost)',

    pipeline_leak_rate AS
      DIV0(pipeline.closed_lost_pipeline, pipeline.beginning_pipeline) * 100
      WITH SYNONYMS = ('leak rate', 'loss rate', 'attrition rate')
      COMMENT = 'Percentage of beginning-of-period pipeline that was lost',

    net_pipeline_movement AS
      pipeline.new_pipeline - pipeline.closed_won_pipeline - pipeline.closed_lost_pipeline
      WITH SYNONYMS = ('net pipeline change', 'pipeline net flow')
      COMMENT = 'Net pipeline movement: new pipeline minus closed won and closed lost',

    -- ===== Multi-path derived metrics =====

    avg_pipeline_per_account_by_account AS
      DIV0(pipeline.total_pipeline_amount_by_account, pipeline.account_count_by_account)
      WITH SYNONYMS = ('pipeline per account by region', 'account average pipeline by region')
      COMMENT = 'Average pipeline amount per account — resolves via accounts path for grouping by account dimensions'
  )

  COMMENT = 'Workiva sales pipeline analytics — tracks opportunity pipeline snapshots with account, contact, solution, and alignment dimensions. Supports YoY analysis, stage duration tracking, pipeline waterfall, and conversion metrics.'

  AI_SQL_GENERATION '
    This semantic view models Workiva sales pipeline data. The fact table (fct_pipeline) contains
    point-in-time snapshots of pipeline, so each opportunity can appear on multiple snapshot dates.

    Key rules:
    - When users ask about "current pipeline" or "latest pipeline", use the latest_pipeline_amount
      or latest_opportunity_count metrics which are semi-additive and avoid double-counting.
    - When users ask about pipeline trends or changes over time, use snapshot_date as the dimension.
    - The pipeline_a_s_usd column represents the pipeline amount already converted to USD.
    - Dimension tables use SCD Type 2 (valid_from/valid_to/is_current) and are joined via range
      relationships, so the correct historical dimension values are automatically resolved.
      This means questions about "team structure as it was in Q3 last year" are answered correctly
      by simply filtering on the snapshot_date — the range joins resolve historical dimension values.
    - Filter out test/internal accounts using is_test_or_internal_account = FALSE unless the user
      specifically asks about them.
    - "Segment", "region", and "sub_region" refer to sales alignment (from dim_opportunity_alignments),
      not account geography. Use account_region for the account geographic region.
    - Users in the Workiva sales org refer to pipeline amounts in millions (e.g. "$50M pipeline").

    Semi-additive metrics and "current" / "latest" questions:
    - When users ask about the current state, latest snapshot, or how things stand today, always prefer
      the semi-additive metrics: latest_pipeline_amount, latest_opportunity_count, latest_account_count.
    - These metrics automatically resolve to the most recent snapshot and avoid double-counting.
    - When users ask for counts or amounts BY a dimension (e.g., "per rep", "by region", "each account"),
      use the semi-additive metric grouped by that dimension. Do NOT use QUALIFY ROW_NUMBER()=1 to pick
      a single latest row — the NON ADDITIVE BY clause handles this correctly.
    - Derived metrics like avg_pipeline_per_account and avg_pipeline_per_opportunity can be broken down
      by any dimension from related tables, including account_region from accounts or region from alignments.

    Stage progression:
    - The standard pipeline stages are: Discovery → Qualification → Proposal → Negotiation → Closed Won / Closed Lost.
    - "Commit" stage means Negotiation or Closed Won (deals the team is confident about).
    - The days_in_current_stage fact tracks how long each deal has been in its current stage.
    - The is_stage_change fact flags snapshots where the stage changed from the prior snapshot.
    - The previous_snapshot_stage fact shows what stage the deal was in on the prior snapshot.

    Pipeline waterfall:
    - pipeline_category classifies each snapshot record as: Beginning (existed at quarter start),
      New (created during the quarter), Closed Won, Closed Lost, Pushed (still open at quarter end
      but expected to close in a future quarter), Expansion (amount increased), Contraction (amount decreased).
    - Use beginning_pipeline, new_pipeline, closed_won_pipeline, closed_lost_pipeline, and pushed_pipeline
      metrics for waterfall analysis.

    Year-over-year:
    - pipeline_amount_yoy gives the pipeline value from 52 snapshots ago (same week last year).
    - pipeline_yoy_change and pipeline_yoy_pct_change give the absolute and percentage differences.
    - For YoY by region or territory, combine these metrics with the region or sub_region dimensions.
  '

  AI_QUESTION_CATEGORIZATION '
    Classify questions into these categories:
    - PIPELINE_SUMMARY: Overall pipeline totals, averages, counts
    - PIPELINE_TRENDS: Period-over-period changes, growth rates, rolling averages, YoY comparisons
    - PIPELINE_WATERFALL: Beginning/new/leaked/pushed/won pipeline movements within a period
    - STAGE_ANALYSIS: Stage duration, stage transitions, velocity, time-in-stage trends
    - CONVERSION_ANALYSIS: Win rates, commit-to-close rates, stage conversion funnels
    - ACCOUNT_ANALYSIS: Pipeline broken down by account, account type, account region
    - SEGMENT_ANALYSIS: Pipeline by segment, region, sub-region (sales alignment)
    - PRODUCT_ANALYSIS: Pipeline by solution category, SKU grouping, booking category
    - REP_ANALYSIS: Pipeline by opportunity owner, creator
    - OPPORTUNITY_DETAIL: Questions about specific opportunities or stages
    - OUT_OF_SCOPE: Questions not related to pipeline analytics
  ';

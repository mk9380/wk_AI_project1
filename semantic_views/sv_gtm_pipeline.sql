-- =============================================================================
-- GTM Pipeline Semantic View (Dimensional Model)
-- Built from fct_pipeline + SCD Type 2 dimension tables
-- Supports: Semi-additive metrics, period-over-period (YoY/QoQ/MoM),
--           multi-path joins, and AI/Cortex Agent optimization
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW {{DATABASE}}.{{SCHEMA}}.SV_GTM_PIPELINE

  -- =========================================================================
  -- TABLES
  -- =========================================================================
  TABLES (
    -- Fact: Daily pipeline snapshots (one row per snapshot_date + opportunity + solution_total)
    pipeline AS {{DATABASE}}.{{SCHEMA}}.FCT_PIPELINE
      PRIMARY KEY (pipeline_key)
      COMMENT = 'Daily pipeline snapshot fact table. Each row represents one opportunity-solution combination on a specific snapshot date. Grain: snapshot_date + opportunity_id + solution_total_id.',

    -- Dimension: Opportunities (SCD Type 2)
    opportunities AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITIES
      PRIMARY KEY (opportunity_id)
      CONSTRAINT opp_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 opportunity dimension. Tracks historical changes to opportunity attributes like stage, type, owner, and dates.',

    -- Dimension: Accounts (SCD Type 2)
    accounts AS {{DATABASE}}.{{SCHEMA}}.DIM_ACCOUNTS
      PRIMARY KEY (account_id)
      CONSTRAINT acct_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 account dimension. Tracks historical changes to account attributes like region, segment, type, and industry.',

    -- Dimension: Users - Opportunity Owner (SCD Type 2)
    opp_owner AS {{DATABASE}}.{{SCHEMA}}.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT owner_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension aliased for the opportunity owner. Provides owner name, role, department, division, and manager.',

    -- Dimension: Users - Opportunity Creator (SCD Type 2)
    opp_creator AS {{DATABASE}}.{{SCHEMA}}.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT creator_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension aliased for the opportunity creator.',

    -- Dimension: Users - Owner Manager (SCD Type 2)
    owner_manager AS {{DATABASE}}.{{SCHEMA}}.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT mgr_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension aliased for the opportunity owner manager.',

    -- Dimension: Opportunity Alignments (SCD Type 2)
    alignments AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_ALIGNMENTS
      PRIMARY KEY (opportunity_id)
      CONSTRAINT align_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 alignment dimension. Tracks market segment, team, region, and sub-region assignments for opportunities.',

    -- Dimension: Solution Classifications (SCD Type 2)
    solution_classifications AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_SOLUTION_CLASSIFICATIONS
      PRIMARY KEY (opportunity_solution_classification_key)
      CONSTRAINT solclass_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 solution classification dimension. Tracks solution category, booking category, SKU grouping, persona, and reporting classification.',

    -- Dimension: Contacts (SCD Type 2)
    contacts AS {{DATABASE}}.{{SCHEMA}}.DIM_CONTACTS
      PRIMARY KEY (contact_id)
      CONSTRAINT contact_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 contact dimension. Tracks contact name, title, lead source, and account association.',

    -- Dimension: Opportunity Partners (SCD Type 2)
    partners AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_PARTNERS
      PRIMARY KEY (opportunity_id)
      CONSTRAINT partner_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 partner dimension. Tracks up to 8 partners per opportunity with relationship type, region, and status.',

    -- Dimension: Opportunity Line Item Summary (SCD Type 2)
    line_items AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_LINE_ITEM_SUMMARY
      PRIMARY KEY (opportunity_id)
      CONSTRAINT li_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 line item summary. Provides aggregated product names and subscription product lists per opportunity.',

    -- Dimension: Opportunity Assist Summary (SCD Type 2)
    assists AS {{DATABASE}}.{{SCHEMA}}.DIM_OPPORTUNITY_ASSIST_SUMMARY
      PRIMARY KEY (opportunity_id)
      CONSTRAINT assist_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 assist summary. Tracks value management and assist engagement details per opportunity.'
  )

  -- =========================================================================
  -- RELATIONSHIPS
  -- All SCD2 dimension joins use snapshot_end_of_day_timestamp as the
  -- temporal key from fct_pipeline, matched against version_start_at /
  -- version_end_at ranges on each dimension to get point-in-time attributes.
  -- =========================================================================
  RELATIONSHIPS (
    -- Pipeline -> Opportunities (temporal)
    pipeline_to_opportunities AS
      pipeline(opportunity_id, snapshot_end_of_day_timestamp)
        REFERENCES opportunities(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Accounts (temporal)
    pipeline_to_accounts AS
      pipeline(customer_account_id, snapshot_end_of_day_timestamp)
        REFERENCES accounts(account_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Opportunity Owner (temporal, multi-path user)
    pipeline_to_owner AS
      pipeline(opportunity_owner_user_id, snapshot_end_of_day_timestamp)
        REFERENCES opp_owner(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Opportunity Creator (temporal, multi-path user)
    pipeline_to_creator AS
      pipeline(opportunity_created_by_user_id, snapshot_end_of_day_timestamp)
        REFERENCES opp_creator(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Solution Classifications (temporal)
    pipeline_to_solution_classifications AS
      pipeline(opportunity_solution_classification_key, snapshot_end_of_day_timestamp)
        REFERENCES solution_classifications(opportunity_solution_classification_key, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Contacts (temporal)
    pipeline_to_contacts AS
      pipeline(first_associated_contact_id, snapshot_end_of_day_timestamp)
        REFERENCES contacts(contact_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Alignments (temporal, joined via opportunity_id)
    pipeline_to_alignments AS
      pipeline(opportunity_id, snapshot_end_of_day_timestamp)
        REFERENCES alignments(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Partners (temporal, joined via opportunity_id)
    pipeline_to_partners AS
      pipeline(opportunity_id, snapshot_end_of_day_timestamp)
        REFERENCES partners(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Line Item Summary (temporal, joined via opportunity_id)
    pipeline_to_line_items AS
      pipeline(opportunity_id, snapshot_end_of_day_timestamp)
        REFERENCES line_items(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Pipeline -> Assist Summary (temporal, joined via opportunity_id)
    pipeline_to_assists AS
      pipeline(opportunity_id, snapshot_end_of_day_timestamp)
        REFERENCES assists(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Owner -> Owner's Manager (temporal, chained from owner)
    owner_to_manager AS
      opp_owner(manager_user_id, version_start_at)
        REFERENCES owner_manager(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE)
  )

  -- =========================================================================
  -- FACTS
  -- =========================================================================
  FACTS (
    -- Core pipeline amounts
    PRIVATE pipeline.f_snapshot_timestamp AS snapshot_end_of_day_timestamp
      COMMENT = 'Snapshot timestamp used for temporal range joins to SCD2 dimensions',

    pipeline.f_qualified_pipeline_amt AS qualified_first_year_s_s_net_amt_usd
      COMMENT = 'First Year S&S Net Amount (USD) for opportunities in a qualified stage (Discovery, Alignment, Validate, Propose, Negotiate and Close, Define Solution)',

    pipeline.f_unqualified_pipeline_amt AS unqualified_first_year_s_s_net_amt_usd
      COMMENT = 'First Year S&S Net Amount (USD) for opportunities in an unqualified stage',

    pipeline.f_total_pipeline_amt AS total_first_year_s_s_net_amt_usd
      COMMENT = 'First Year S&S Net Amount (USD) representing total pipeline (qualified + unqualified)',

    -- Pipeline status flags
    pipeline.f_is_open AS is_open
      COMMENT = 'TRUE if the opportunity is in an open stage at this snapshot date',

    pipeline.f_is_closed_won AS is_closed_won
      COMMENT = 'TRUE if the opportunity is closed won at this snapshot date',

    pipeline.f_is_closed_pending AS is_closed_pending
      COMMENT = 'TRUE if the opportunity is closed pending at this snapshot date',

    pipeline.f_is_closed_lost AS is_closed_lost
      COMMENT = 'TRUE if the opportunity is closed lost at this snapshot date',

    pipeline.f_is_qualified AS is_qualified
      COMMENT = 'TRUE if the opportunity is in a qualified stage at this snapshot date',

    pipeline.f_is_opportunity_with_solution_total AS is_opportunity_with_solution_total
      COMMENT = 'TRUE if the opportunity has a solution total record',

    -- Snapshot identification flags
    pipeline.f_is_current_day_snapshot AS is_current_day_snapshot
      COMMENT = 'TRUE if this snapshot_date is the current calendar day',

    pipeline.f_is_quarterly_snapshot AS is_quarterly_snapshot
      COMMENT = 'TRUE if this is a key date for quarterly pipeline reporting (5th business day of quarter or current day)',

    pipeline.f_is_monthly_snapshot AS is_monthly_snapshot
      COMMENT = 'TRUE if this is a key date for monthly pipeline reporting (5th business day of month or current day)',

    pipeline.f_is_fifth_bday_quarter AS is_fifth_business_day_of_quarter_snapshot
      COMMENT = 'TRUE if this snapshot_date is the 5th business day of the quarter',

    pipeline.f_is_fifth_bday_month AS is_fifth_business_day_of_month_snapshot
      COMMENT = 'TRUE if this snapshot_date is the 5th business day of the month',

    pipeline.f_is_first_day_of_quarter AS is_first_day_of_quarter_snapshot
      COMMENT = 'TRUE if this snapshot_date is the first day of the quarter',

    pipeline.f_is_first_day_of_month AS is_first_day_of_month_snapshot
      COMMENT = 'TRUE if this snapshot_date is the first day of the month',

    pipeline.f_is_one_year_ago AS is_one_year_ago_snapshot
      COMMENT = 'TRUE if this snapshot_date is exactly one year prior to today',

    pipeline.f_is_two_years_ago AS is_two_years_ago_snapshot
      COMMENT = 'TRUE if this snapshot_date is exactly two years prior to today',

    pipeline.f_is_three_years_ago AS is_three_years_ago_snapshot
      COMMENT = 'TRUE if this snapshot_date is exactly three years prior to today',

    pipeline.f_is_snapshot_near_closed AS is_snapshot_near_closed_date
      COMMENT = 'TRUE if the snapshot date falls within 1 year before and 2 years after the opportunity closed date',

    -- Metric flags from fct_pipeline
    pipeline.f_is_metric_created_pipeline AS is_metric_created_pipeline
      COMMENT = 'TRUE if this record qualifies for the created pipeline metric',

    pipeline.f_is_metric_open_qualified AS is_metric_open_qualified_pipeline
      COMMENT = 'TRUE if this record qualifies for the open qualified pipeline metric',

    pipeline.f_is_metric_open_total AS is_metric_open_total_pipeline
      COMMENT = 'TRUE if this record qualifies for the open total pipeline metric'
  )

  -- =========================================================================
  -- DIMENSIONS
  -- =========================================================================
  DIMENSIONS (
    -- === Time Dimensions (from pipeline fact) ===
    pipeline.snapshot_date_dim AS snapshot_date
      WITH SYNONYMS = ('pipeline date', 'snapshot', 'as of date')
      COMMENT = 'Calendar date of the pipeline snapshot. Use this to filter or group pipeline by date.',

    pipeline.snapshot_year_quarter_dim AS snapshot_year_quarter
      WITH SYNONYMS = ('quarter', 'fiscal quarter', 'year quarter')
      COMMENT = 'Fiscal year and quarter label for the snapshot (e.g. FY25 Q1)',

    pipeline.snapshot_year_dim AS YEAR(snapshot_date)
      WITH SYNONYMS = ('year', 'snapshot year', 'fiscal year')
      COMMENT = 'Calendar year of the pipeline snapshot',

    pipeline.snapshot_quarter_dim AS QUARTER(snapshot_date)
      WITH SYNONYMS = ('quarter number')
      COMMENT = 'Quarter number (1-4) of the pipeline snapshot',

    pipeline.snapshot_month_dim AS TO_VARCHAR(snapshot_date, 'YYYY-MM')
      WITH SYNONYMS = ('month', 'snapshot month')
      COMMENT = 'Year-month of the pipeline snapshot (e.g. 2025-03)',

    pipeline.snapshot_week_dim AS TO_VARCHAR(DATE_TRUNC('WEEK', snapshot_date), 'YYYY-MM-DD')
      WITH SYNONYMS = ('week', 'snapshot week')
      COMMENT = 'Week start date of the pipeline snapshot',

    -- === Snapshot Flag Dimensions ===
    pipeline.is_current_day_snapshot_dim AS is_current_day_snapshot
      COMMENT = 'TRUE if this snapshot is for the current calendar day. Filter to TRUE for latest pipeline state.',

    pipeline.is_quarterly_snapshot_dim AS is_quarterly_snapshot
      COMMENT = 'TRUE for key quarterly reporting dates (5th business day of quarter or current day)',

    pipeline.is_monthly_snapshot_dim AS is_monthly_snapshot
      COMMENT = 'TRUE for key monthly reporting dates (5th business day of month or current day)',

    pipeline.is_fifth_bday_quarter_dim AS is_fifth_business_day_of_quarter_snapshot
      COMMENT = 'TRUE if this snapshot is the 5th business day of the quarter',

    pipeline.is_fifth_bday_month_dim AS is_fifth_business_day_of_month_snapshot
      COMMENT = 'TRUE if this snapshot is the 5th business day of the month',

    pipeline.is_first_day_of_quarter_dim AS is_first_day_of_quarter_snapshot
      COMMENT = 'TRUE if this snapshot is the first day of the quarter',

    pipeline.is_first_day_of_month_dim AS is_first_day_of_month_snapshot
      COMMENT = 'TRUE if this snapshot is the first day of the month',

    pipeline.is_one_year_ago_dim AS is_one_year_ago_snapshot
      COMMENT = 'TRUE if this snapshot is exactly one year ago from today',

    pipeline.is_two_years_ago_dim AS is_two_years_ago_snapshot
      COMMENT = 'TRUE if this snapshot is exactly two years ago from today',

    pipeline.is_three_years_ago_dim AS is_three_years_ago_snapshot
      COMMENT = 'TRUE if this snapshot is exactly three years ago from today',

    pipeline.is_snapshot_near_closed_dim AS is_snapshot_near_closed_date
      COMMENT = 'TRUE if the snapshot date is within 1 year before and 2 years after the opportunity closed date',

    -- === Pipeline Status Dimensions ===
    pipeline.is_open_dim AS is_open
      WITH SYNONYMS = ('open pipeline', 'active pipeline')
      COMMENT = 'TRUE if the opportunity is in an open (active) stage at this snapshot',

    pipeline.is_closed_won_dim AS is_closed_won
      WITH SYNONYMS = ('won', 'closed won')
      COMMENT = 'TRUE if the opportunity is closed won at this snapshot',

    pipeline.is_closed_lost_dim AS is_closed_lost
      WITH SYNONYMS = ('lost', 'closed lost')
      COMMENT = 'TRUE if the opportunity is closed lost at this snapshot',

    pipeline.is_closed_pending_dim AS is_closed_pending
      WITH SYNONYMS = ('pending', 'closed pending')
      COMMENT = 'TRUE if the opportunity is closed pending at this snapshot',

    pipeline.is_qualified_dim AS is_qualified
      WITH SYNONYMS = ('qualified', 'qualified pipeline')
      COMMENT = 'TRUE if the opportunity is in a qualified stage (Discovery, Alignment, Validate, Propose, Negotiate and Close, Define Solution)',

    pipeline.opportunity_stage_name_dim AS opportunity_stage_name
      WITH SYNONYMS = ('stage', 'opp stage', 'deal stage', 'sales stage')
      COMMENT = 'Stage of the opportunity at this snapshot (e.g. Discovery, Propose, Negotiate and Close, Closed Won)',

    pipeline.large_deal_category_dim AS large_deal_category
      COMMENT = 'Large deal size classification category',

    -- === Pipeline Identifier Dimensions ===
    pipeline.pipeline_key_dim AS pipeline_key
      COMMENT = 'Unique surrogate key for each pipeline snapshot record (snapshot_date + opportunity_id + solution_total_id)',

    pipeline.opportunity_id_dim AS opportunity_id
      WITH SYNONYMS = ('opp id', 'deal id')
      COMMENT = 'Salesforce opportunity identifier',

    -- === Opportunity Dimensions (from dim_opportunities via temporal join) ===
    opportunities.opportunity_name_dim AS opportunity_name
      WITH SYNONYMS = ('opp name', 'deal name')
      COMMENT = 'Name of the opportunity as of the snapshot date',

    opportunities.opportunity_type_dim AS opportunity_type
      WITH SYNONYMS = ('opp type', 'deal type')
      COMMENT = 'Type of opportunity (e.g. New Deal, Renewal, Price Increase)',

    opportunities.opportunity_deal_type_dim AS opportunity_deal_type
      COMMENT = 'High-level deal classification (e.g. SaaS, Consulting, Government)',

    opportunities.opportunity_reporting_sub_type_dim AS opportunity_reporting_sub_type
      COMMENT = 'Reporting sub-type of the opportunity (e.g. New Deal, Renewal)',

    opportunities.opportunity_record_type_name_dim AS opportunity_record_type_name
      WITH SYNONYMS = ('record type')
      COMMENT = 'Salesforce record type (e.g. Direct, New Deal, Price Increase)',

    opportunities.opportunity_forecast_category_name_dim AS opportunity_forecast_category_name
      WITH SYNONYMS = ('forecast category', 'commit category')
      COMMENT = 'Forecast category (e.g. Commit, Upside, Best Case, Pipeline)',

    opportunities.opportunity_closed_date_dim AS opportunity_closed_date
      WITH SYNONYMS = ('close date', 'expected close date')
      COMMENT = 'Expected or actual close date of the opportunity',

    opportunities.opportunity_created_date_dim AS opportunity_created_date
      WITH SYNONYMS = ('created date', 'opp created date')
      COMMENT = 'Date the opportunity was created',

    opportunities.opportunity_qualified_date_dim AS opportunity_qualified_date
      WITH SYNONYMS = ('qualified date')
      COMMENT = 'Date the opportunity first reached a qualified stage',

    opportunities.opportunity_bookings_date_dim AS opportunity_bookings_date
      WITH SYNONYMS = ('booking date')
      COMMENT = 'Date the revenue was booked',

    opportunities.opportunity_booking_or_closed_date_dim AS opportunity_booking_or_closed_date
      WITH SYNONYMS = ('booking or closed date')
      COMMENT = 'Booking date if available, otherwise the closed date',

    opportunities.is_deal_registration_dim AS is_deal_registration
      COMMENT = 'TRUE if this opportunity is a deal registration',

    opportunities.is_alignment_stage_plus_dim AS is_alignment_stage_plus
      COMMENT = 'TRUE if the opportunity is at or past alignment stage',

    -- === Account Dimensions (from dim_accounts via temporal join) ===
    accounts.account_name_dim AS account_name
      WITH SYNONYMS = ('company name', 'customer name', 'company')
      COMMENT = 'Name of the customer account as of the snapshot date',

    accounts.account_region_dim AS account_region
      WITH SYNONYMS = ('account geo', 'customer region')
      COMMENT = 'Geographic region of the account (e.g. North America, EMEA, APAC, LATAM)',

    accounts.account_segment_dim AS account_segment
      WITH SYNONYMS = ('account segment', 'customer segment')
      COMMENT = 'Business segment of the account (e.g. Strategic, Corporate - Expansion)',

    accounts.account_type_dim AS account_type
      COMMENT = 'Account type (e.g. Publicly Traded Company, Private)',

    accounts.account_corporate_country_dim AS account_corporate_country
      WITH SYNONYMS = ('country', 'customer country')
      COMMENT = 'Country of the account corporation',

    accounts.account_workiva_internal_industry_dim AS account_workiva_internal_industry
      WITH SYNONYMS = ('industry', 'internal industry')
      COMMENT = 'Workiva internal industry classification (e.g. Banking, Insurance, Investments)',

    accounts.account_industry_group_dim AS account_industry_group
      COMMENT = 'Industry group classification of the account',

    accounts.account_owner_role_dim AS account_owner_role
      COMMENT = 'Role of the account owner',

    accounts.is_financial_industry_dim AS is_financial_industry
      COMMENT = 'TRUE if the account is in the financial industry',

    accounts.is_energy_industry_dim AS is_energy_industry
      COMMENT = 'TRUE if the account is in the energy industry',

    accounts.financial_industry_type_dim AS financial_industry_type
      COMMENT = 'Type of financial industry (e.g. Insurance, Banking, Investments)',

    accounts.energy_industry_type_dim AS energy_industry_type
      COMMENT = 'Type of energy industry (e.g. Oil & Gas, Utilities)',

    accounts.is_test_or_internal_account_dim AS is_test_or_internal_account
      COMMENT = 'TRUE if this is a test or internal account. Filter to FALSE to exclude test data.',

    accounts.is_partner_dim AS is_partner_account
      COMMENT = 'TRUE if this account is a partner',

    accounts.is_customer_dim AS is_customer
      COMMENT = 'TRUE if this account is a current customer',

    accounts.account_csrd_dim AS account_csrd
      COMMENT = 'CSRD (Corporate Sustainability Reporting Directive) classification',

    accounts.account_wk_company_type_dim AS account_wk_company_type
      COMMENT = 'Workiva company type classification',

    -- Ultimate Parent Account dimensions
    accounts.ultimate_parent_account_name_dim AS ultimate_parent_account_name
      WITH SYNONYMS = ('parent account', 'parent company', 'ultimate parent')
      COMMENT = 'Name of the ultimate parent account',

    accounts.ultimate_parent_account_type_dim AS ultimate_parent_account_type
      COMMENT = 'Account type of the ultimate parent',

    accounts.ultimate_parent_region_dim AS ultimate_parent_region
      COMMENT = 'Region of the ultimate parent account',

    accounts.ultimate_parent_segment_dim AS ultimate_parent_segment
      COMMENT = 'Business segment of the ultimate parent account',

    accounts.ultimate_parent_corporate_country_dim AS ultimate_parent_corporate_country
      COMMENT = 'Corporate country of the ultimate parent',

    accounts.ultimate_parent_csrd_dim AS ultimate_parent_csrd
      COMMENT = 'CSRD classification of the ultimate parent',

    accounts.ultimate_parent_customer_flag_dim AS ultimate_parent_customer_flag
      COMMENT = 'TRUE if the ultimate parent is a customer',

    accounts.ultimate_parent_industry_group_dim AS ultimate_parent_industry_group
      COMMENT = 'Industry group of the ultimate parent',

    -- === Alignment Dimensions (from dim_opportunity_alignments via temporal join) ===
    alignments.opportunity_market_segment_dim AS opportunity_market_segment
      WITH SYNONYMS = ('market segment', 'segment', 'sales segment')
      COMMENT = 'Sales segment aligned to the opportunity, derived from the closer/owner segment',

    alignments.opportunity_team_dim AS opportunity_team
      WITH SYNONYMS = ('team', 'sales team')
      COMMENT = 'Sales team aligned to the opportunity',

    alignments.opportunity_owner_region_dim AS opportunity_owner_region
      WITH SYNONYMS = ('owner region', 'sales region', 'region')
      COMMENT = 'Region aligned to the opportunity, derived from the closer/owner region',

    alignments.opportunity_owner_sub_region_dim AS opportunity_owner_sub_region
      WITH SYNONYMS = ('sub region', 'sub-region', 'territory')
      COMMENT = 'Sub-region aligned to the opportunity',

    alignments.public_type_dim AS public_type
      COMMENT = 'Public sector type (SLED or Federal) if applicable, otherwise NULL',

    alignments.is_public_type_dim AS is_public_type
      COMMENT = 'TRUE if the opportunity team is a public sector designation (SLED or Federal)',

    alignments.opportunity_alignment_category_dim AS opportunity_alignment_category
      COMMENT = 'Aggregated alignment category (e.g. North America, EMEA, Global MSP)',

    alignments.opportunity_alignment_sub_category_dim AS opportunity_alignment_sub_category
      COMMENT = 'Alignment sub-category derived from market segment',

    alignments.opportunity_alignment_region_dim AS opportunity_alignment_region
      COMMENT = 'Alignment region derived from category and market segment',

    -- === Solution Classification Dimensions (via temporal join) ===
    solution_classifications.solution_category_dim AS solution_category
      WITH SYNONYMS = ('product category', 'solution')
      COMMENT = 'Category of the solution being sold (e.g. SEC/SEDAR, ESG, Controls Management)',

    solution_classifications.booking_category_dim AS booking_category
      WITH SYNONYMS = ('booking type')
      COMMENT = 'Booking category for the solution (e.g. Management Reporting, Sustainability)',

    solution_classifications.solution_group_dim AS solution_group
      WITH SYNONYMS = ('product group')
      COMMENT = 'Logical grouping of the solution (e.g. Financial Reporting, GRC, Sustainability)',

    solution_classifications.sku_grouping_dim AS sku_grouping
      WITH SYNONYMS = ('product grouping', 'sku group')
      COMMENT = 'SKU grouping of the solution',

    solution_classifications.solution_picklist_dim AS solution_picklist
      COMMENT = 'Solution picklist value from Salesforce',

    solution_classifications.persona_dim AS persona
      WITH SYNONYMS = ('target persona', 'buyer persona')
      COMMENT = 'Business persona targeted by the solution (e.g. Accounting & Finance, Sustainability)',

    solution_classifications.reporting_classification_pipeline_dim AS reporting_classification
      WITH SYNONYMS = ('reporting category', 'classification')
      COMMENT = 'Reporting classification for pipeline (e.g. NL, NS, Reno, PI-Comm Ops)',

    solution_classifications.is_multi_category_dim AS is_multi_category
      COMMENT = 'TRUE if the opportunity spans multiple solution groups (Financial Reporting, GRC, Sustainability)',

    solution_classifications.is_multi_solution_category_dim AS is_multi_solution_category
      COMMENT = 'TRUE if the opportunity contains more than one distinct solution category',

    solution_classifications.is_advanced_sku_grouping_dim AS is_advanced_sku_grouping
      COMMENT = 'TRUE if the opportunity has at least one Advanced SKU grouping',

    -- === Opportunity Owner Dimensions (multi-path via pipeline_to_owner) ===
    opp_owner.user_name_dim AS opportunity_owner_name
      WITH SYNONYMS = ('rep name', 'owner name', 'sales rep', 'closer name')
      COMMENT = 'Name of the opportunity owner/closer as of the snapshot date',

    opp_owner.user_role_dim AS opportunity_owner_role
      WITH SYNONYMS = ('owner role', 'rep role')
      COMMENT = 'Role of the opportunity owner',

    opp_owner.user_department_dim AS opportunity_owner_department
      COMMENT = 'Department of the opportunity owner',

    opp_owner.user_division_dim AS opportunity_owner_division
      COMMENT = 'Division of the opportunity owner',

    -- === Opportunity Creator Dimensions (multi-path via pipeline_to_creator) ===
    opp_creator.user_name_dim AS opportunity_creator_name
      WITH SYNONYMS = ('creator name')
      COMMENT = 'Name of the user who created the opportunity',

    -- === Owner Manager Dimensions (chained via owner_to_manager) ===
    owner_manager.user_name_dim AS owner_manager_name
      WITH SYNONYMS = ('manager name', 'manager')
      COMMENT = 'Name of the opportunity owner manager',

    owner_manager.user_role_dim AS owner_manager_role
      COMMENT = 'Role of the opportunity owner manager',

    -- === Contact Dimensions (via temporal join) ===
    contacts.contact_full_name_dim AS contact_name
      WITH SYNONYMS = ('contact', 'primary contact')
      COMMENT = 'Full name of the first associated contact',

    contacts.contact_title_dim AS contact_title
      COMMENT = 'Job title of the primary contact',

    contacts.contact_lead_source_dim AS contact_lead_source
      COMMENT = 'Lead source of the primary contact',

    -- === Partner Dimensions (via temporal join) ===
    partners.partner_1_account_name_dim AS partner_1_name
      WITH SYNONYMS = ('primary partner', 'partner name')
      COMMENT = 'Name of the primary partner on the opportunity',

    partners.partner_1_relationship_dim AS partner_1_relationship
      COMMENT = 'Relationship type of the primary partner',

    partners.partner_1_type_dim AS partner_1_type
      COMMENT = 'Type of the primary partner',

    partners.partner_1_account_region_dim AS partner_1_region
      COMMENT = 'Region of the primary partner',

    -- === Line Item Dimensions ===
    line_items.opportunity_products_dim AS opportunity_products
      WITH SYNONYMS = ('products', 'product list')
      COMMENT = 'Comma-separated list of all distinct product names on this opportunity',

    -- === Assist Dimensions ===
    assists.has_completed_value_management_assist_dim AS has_completed_value_management_assist
      COMMENT = 'TRUE if a value management assist has been completed for this opportunity',

    assists.latest_vm_assist_value_stage_dim AS latest_vm_assist_value_stage
      COMMENT = 'Value stage of the latest value management assist'
  )

  -- =========================================================================
  -- METRICS
  -- =========================================================================
  METRICS (
    -- ===== Core Pipeline Aggregation Metrics =====

    pipeline.total_pipeline AS SUM(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('total pipeline', 'pipeline value', 'total pipe', 'all pipeline')
      COMMENT = 'Total pipeline amount (USD) including qualified and unqualified stages',

    pipeline.qualified_pipeline AS SUM(qualified_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('qualified pipe', 'qualified pipeline amount')
      COMMENT = 'Pipeline amount (USD) for opportunities in qualified stages only',

    pipeline.unqualified_pipeline AS SUM(unqualified_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('unqualified pipe', 'unqualified pipeline amount')
      COMMENT = 'Pipeline amount (USD) for opportunities in unqualified stages only',

    pipeline.avg_pipeline_amount AS AVG(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('average deal size', 'avg deal value')
      COMMENT = 'Average pipeline amount per record (USD)',

    pipeline.opportunity_count AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('number of opportunities', 'deal count', 'opp count')
      COMMENT = 'Count of distinct opportunities in pipeline',

    pipeline.qualified_opportunity_count AS COUNT(DISTINCT CASE WHEN is_qualified THEN opportunity_id END)
      WITH SYNONYMS = ('qualified opp count', 'qualified deals')
      COMMENT = 'Count of distinct opportunities in a qualified stage',

    pipeline.unqualified_opportunity_count AS COUNT(DISTINCT CASE WHEN NOT is_qualified THEN opportunity_id END)
      WITH SYNONYMS = ('unqualified opp count', 'unqualified deals')
      COMMENT = 'Count of distinct opportunities in an unqualified stage',

    pipeline.account_count AS COUNT(DISTINCT customer_account_id)
      WITH SYNONYMS = ('number of accounts', 'customer count')
      COMMENT = 'Count of distinct accounts in pipeline',

    pipeline.pipeline_record_count AS COUNT(pipeline_key)
      WITH SYNONYMS = ('record count', 'snapshot record count')
      COMMENT = 'Count of pipeline snapshot records (each opportunity-solution per snapshot date)',

    -- ===== Semi-Additive Metrics (snapshot-safe, avoid double-counting across dates) =====

    pipeline.latest_total_pipeline
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('current pipeline', 'latest pipe', 'current total pipeline')
      COMMENT = 'Total pipeline from the latest snapshot date. Use for current-state pipeline totals. Avoids double-counting across snapshots.',

    pipeline.latest_qualified_pipeline
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(qualified_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('current qualified pipeline', 'latest qualified pipe')
      COMMENT = 'Qualified pipeline from the latest snapshot. Use for current-state qualified totals.',

    pipeline.latest_unqualified_pipeline
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(unqualified_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('current unqualified pipeline')
      COMMENT = 'Unqualified pipeline from the latest snapshot.',

    pipeline.latest_opportunity_count
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('current opportunity count', 'current deal count')
      COMMENT = 'Distinct opportunity count from the latest snapshot',

    pipeline.latest_qualified_opportunity_count
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT CASE WHEN is_qualified THEN opportunity_id END)
      WITH SYNONYMS = ('current qualified opp count')
      COMMENT = 'Distinct qualified opportunity count from the latest snapshot',

    pipeline.latest_account_count
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT customer_account_id)
      WITH SYNONYMS = ('current account count', 'active accounts')
      COMMENT = 'Distinct account count from the latest snapshot',

    -- ===== Multi-Path Metrics (account path) =====

    pipeline.latest_total_pipeline_by_account
      USING (pipeline_to_accounts)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('pipeline by account', 'account pipeline')
      COMMENT = 'Latest total pipeline resolved via accounts path. Use when grouping by account dimensions.',

    pipeline.latest_opportunity_count_by_account
      USING (pipeline_to_accounts)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      COMMENT = 'Latest opportunity count resolved via accounts path',

    pipeline.total_pipeline_by_account
      USING (pipeline_to_accounts)
      AS SUM(total_first_year_s_s_net_amt_usd)
      COMMENT = 'Total pipeline resolved via accounts path for cross-snapshot analysis by account dimensions',

    -- ===== Multi-Path Metrics (owner user path) =====

    pipeline.latest_total_pipeline_by_owner
      USING (pipeline_to_owner)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('pipeline by rep', 'rep pipeline', 'owner pipeline')
      COMMENT = 'Latest total pipeline resolved via opportunity owner. Use when grouping by owner/rep dimensions.',

    pipeline.latest_opportunity_count_by_owner
      USING (pipeline_to_owner)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('opportunities by rep', 'rep opp count')
      COMMENT = 'Latest opportunity count resolved via opportunity owner',

    -- ===== Multi-Path Metrics (alignment path) =====

    pipeline.latest_total_pipeline_by_alignment
      USING (pipeline_to_alignments)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS SUM(total_first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('pipeline by segment', 'segment pipeline')
      COMMENT = 'Latest total pipeline resolved via alignments. Use when grouping by segment, team, or region.',

    pipeline.latest_opportunity_count_by_alignment
      USING (pipeline_to_alignments)
      NON ADDITIVE BY (pipeline.snapshot_date_dim)
      AS COUNT(DISTINCT opportunity_id)
      COMMENT = 'Latest opportunity count resolved via alignments',

    -- ===== Window Metrics: Year-over-Year (YoY) =====

    pipeline.total_pipeline_yoy AS LAG(pipeline.total_pipeline, 365)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_year_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('pipeline same day last year', 'prior year pipeline', 'yoy pipeline')
      COMMENT = 'Total pipeline from the same day one year ago (365 snapshots back). Requires snapshot_date_dim and snapshot_year_dim in query.',

    pipeline.qualified_pipeline_yoy AS LAG(pipeline.qualified_pipeline, 365)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_year_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('qualified pipeline last year')
      COMMENT = 'Qualified pipeline from the same day one year ago',

    pipeline.opportunity_count_yoy AS LAG(pipeline.opportunity_count, 365)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_year_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('opp count last year')
      COMMENT = 'Opportunity count from the same day one year ago',

    -- ===== Window Metrics: Quarter-over-Quarter (QoQ) =====

    pipeline.total_pipeline_qoq AS LAG(pipeline.total_pipeline, 91)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_quarter_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('pipeline last quarter', 'prior quarter pipeline', 'qoq pipeline')
      COMMENT = 'Total pipeline from approximately one quarter ago (91 days back). Requires snapshot_date_dim and snapshot_quarter_dim in query.',

    pipeline.qualified_pipeline_qoq AS LAG(pipeline.qualified_pipeline, 91)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_quarter_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('qualified pipeline last quarter')
      COMMENT = 'Qualified pipeline from approximately one quarter ago',

    -- ===== Window Metrics: Month-over-Month (MoM) =====

    pipeline.total_pipeline_mom AS LAG(pipeline.total_pipeline, 30)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_month_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('pipeline last month', 'prior month pipeline', 'mom pipeline')
      COMMENT = 'Total pipeline from approximately one month ago (30 days back). Requires snapshot_date_dim and snapshot_month_dim in query.',

    pipeline.qualified_pipeline_mom AS LAG(pipeline.qualified_pipeline, 30)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_month_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('qualified pipeline last month')
      COMMENT = 'Qualified pipeline from approximately one month ago',

    -- ===== Window Metrics: Rolling Averages =====

    pipeline.total_pipeline_7d_avg AS AVG(pipeline.total_pipeline)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_year_dim
            ORDER BY pipeline.snapshot_date_dim
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)
      WITH SYNONYMS = ('7-day average pipeline', 'rolling weekly pipeline')
      COMMENT = 'Rolling 7-day average of total pipeline amount',

    pipeline.total_pipeline_30d_avg AS AVG(pipeline.total_pipeline)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim, pipeline.snapshot_month_dim
            ORDER BY pipeline.snapshot_date_dim
            RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW)
      WITH SYNONYMS = ('30-day average pipeline', 'rolling monthly pipeline')
      COMMENT = 'Rolling 30-day average of total pipeline amount',

    -- ===== Window Metrics: Previous Snapshot =====

    pipeline.total_pipeline_prev_snapshot AS LAG(pipeline.total_pipeline, 1)
      OVER (PARTITION BY EXCLUDING pipeline.snapshot_date_dim
            ORDER BY pipeline.snapshot_date_dim)
      WITH SYNONYMS = ('previous pipeline', 'prior day pipeline')
      COMMENT = 'Total pipeline from the previous snapshot date',

    -- ===== Derived Metrics (view-level, computed from other metrics) =====

    avg_pipeline_per_account AS
      DIV0(pipeline.total_pipeline, pipeline.account_count)
      WITH SYNONYMS = ('pipeline per account', 'account average pipeline')
      COMMENT = 'Average total pipeline amount per distinct account',

    avg_pipeline_per_opportunity AS
      DIV0(pipeline.total_pipeline, pipeline.opportunity_count)
      WITH SYNONYMS = ('pipeline per opportunity', 'average deal size metric')
      COMMENT = 'Average total pipeline amount per distinct opportunity',

    pipeline_yoy_change AS
      pipeline.total_pipeline - pipeline.total_pipeline_yoy
      WITH SYNONYMS = ('yoy pipeline change', 'year over year change')
      COMMENT = 'Absolute change in total pipeline compared to same day last year',

    pipeline_yoy_pct_change AS
      DIV0(pipeline.total_pipeline - pipeline.total_pipeline_yoy, pipeline.total_pipeline_yoy) * 100
      WITH SYNONYMS = ('yoy pipeline growth', 'year over year percent change')
      COMMENT = 'Percentage change in total pipeline compared to same day last year',

    pipeline_qoq_change AS
      pipeline.total_pipeline - pipeline.total_pipeline_qoq
      WITH SYNONYMS = ('qoq pipeline change', 'quarter over quarter change')
      COMMENT = 'Absolute change in total pipeline compared to approximately one quarter ago',

    pipeline_qoq_pct_change AS
      DIV0(pipeline.total_pipeline - pipeline.total_pipeline_qoq, pipeline.total_pipeline_qoq) * 100
      WITH SYNONYMS = ('qoq pipeline growth', 'quarter over quarter percent change')
      COMMENT = 'Percentage change in total pipeline compared to approximately one quarter ago',

    pipeline_mom_change AS
      pipeline.total_pipeline - pipeline.total_pipeline_mom
      WITH SYNONYMS = ('mom pipeline change', 'month over month change')
      COMMENT = 'Absolute change in total pipeline compared to approximately one month ago',

    pipeline_mom_pct_change AS
      DIV0(pipeline.total_pipeline - pipeline.total_pipeline_mom, pipeline.total_pipeline_mom) * 100
      WITH SYNONYMS = ('mom pipeline growth', 'month over month percent change')
      COMMENT = 'Percentage change in total pipeline compared to approximately one month ago',

    pipeline_snapshot_change AS
      pipeline.total_pipeline - pipeline.total_pipeline_prev_snapshot
      WITH SYNONYMS = ('daily pipeline change', 'snapshot change')
      COMMENT = 'Absolute change in total pipeline from the previous snapshot day'
  )

  COMMENT = 'Workiva GTM Pipeline Analytics — built on the dimensional model (fct_pipeline + SCD Type 2 dimensions). Tracks daily pipeline snapshots with historical account, opportunity, alignment, solution, contact, and partner attributes resolved via temporal range joins. Supports semi-additive metrics for current-state analysis, multi-path metrics for dimensional grouping, and YoY/QoQ/MoM period-over-period comparisons.'

  AI_SQL_GENERATION '
    This semantic view models Workiva Go-To-Market sales pipeline data built on a dimensional model.
    The fact table (fct_pipeline) contains daily point-in-time snapshots, so each opportunity can
    appear on multiple snapshot dates. All dimension tables are SCD Type 2 and joined via temporal
    range joins using snapshot_end_of_day_timestamp, so historical dimension values are automatically
    resolved to the correct point-in-time version.

    CRITICAL RULES FOR SQL GENERATION:

    1. CURRENT STATE QUERIES: When users ask about "current pipeline", "latest pipeline", or
       "how much pipeline do we have", ALWAYS use the semi-additive metrics (latest_total_pipeline,
       latest_qualified_pipeline, latest_opportunity_count, etc.). These automatically resolve to
       the most recent snapshot and prevent double-counting across dates.

    2. SNAPSHOT-BASED QUERIES: When users ask about pipeline "as of" a specific date, or pipeline
       trends over time, use the base metrics (total_pipeline, qualified_pipeline) and filter by
       snapshot_date_dim.

    3. QUALIFIED vs TOTAL: "Pipeline" without qualification typically means qualified_pipeline.
       "Total pipeline" means total_pipeline which includes both qualified and unqualified stages.
       Always clarify if ambiguous.

    4. KEY SNAPSHOT DATES: The 5th business day of each quarter (is_fifth_business_day_of_quarter_snapshot)
       is the standard reporting date for quarterly pipeline reviews. The 5th business day of the month
       (is_fifth_business_day_of_month_snapshot) is used for monthly reviews.

    5. PERIOD-OVER-PERIOD: For YoY comparisons use total_pipeline_yoy and pipeline_yoy_pct_change.
       For QoQ use total_pipeline_qoq and pipeline_qoq_pct_change.
       For MoM use total_pipeline_mom and pipeline_mom_pct_change.
       These window metrics REQUIRE snapshot_date_dim in the query dimensions.

    6. MULTI-PATH RESOLUTION: When grouping by account dimensions (account_name, account_region, etc.),
       prefer latest_total_pipeline_by_account or total_pipeline_by_account.
       When grouping by owner/rep dimensions, prefer latest_total_pipeline_by_owner.
       When grouping by segment/region/team, prefer latest_total_pipeline_by_alignment.

    7. TEST ACCOUNTS: Filter out test/internal accounts with is_test_or_internal_account = FALSE
       unless the user specifically asks about test data.

    8. SALES ALIGNMENT vs ACCOUNT GEO: "Segment", "region", "sub_region", and "team" refer to sales
       alignment (from dim_opportunity_alignments), NOT account geography. Use account_region for
       the account geographic region. Use opportunity_owner_region for the sales region.

    9. PIPELINE AMOUNTS: All amounts are in USD (first_year_s_s_net_amt_usd). Users in the Workiva
       sales org often refer to pipeline in millions (e.g. "$50M pipeline").

    10. OPPORTUNITY STAGES: Standard pipeline stages are Discovery, Alignment, Validate, Propose,
        Define Solution, Negotiate and Close, Closed Won, Closed Lost/No Decision. Qualified stages
        include Discovery, Alignment, Validate, Propose, Define Solution, and Negotiate and Close.

    11. REPORTING CLASSIFICATION: Values include NL (New Logo), NS (New Solution), Reno (Renegotiation),
        PI-Comm Ops (Price Increase). These come from reporting_classification dimension.

    12. SNAPSHOT DATE FORMATS: snapshot_year_quarter uses format like "FY25 Q1".
        Fiscal year follows standard calendar year.
  '

  AI_QUESTION_CATEGORIZATION '
    Classify questions into these categories:
    - PIPELINE_CURRENT_STATE: Current pipeline totals, latest snapshot values, "how much pipeline"
    - PIPELINE_TRENDS: Period-over-period changes, growth rates, rolling averages, YoY/QoQ/MoM
    - PIPELINE_BY_SEGMENT: Pipeline by market segment, team, region, sub-region (sales alignment)
    - PIPELINE_BY_ACCOUNT: Pipeline by account name, account region, account type, industry
    - PIPELINE_BY_PRODUCT: Pipeline by solution category, solution group, SKU grouping, persona
    - PIPELINE_BY_REP: Pipeline by opportunity owner, creator, manager
    - PIPELINE_BY_STAGE: Pipeline by opportunity stage, forecast category, qualified vs unqualified
    - PIPELINE_SNAPSHOT_COMPARISON: Comparing pipeline at specific dates (5th business day, quarter starts)
    - PARTNER_ANALYSIS: Pipeline involving partners
    - OPPORTUNITY_DETAIL: Questions about specific opportunities or deals
    - OUT_OF_SCOPE: Questions about bookings, revenue, contracts, or topics not in pipeline data.
      Redirect to the GTM Bookings semantic view for booking-related questions.
  '
;

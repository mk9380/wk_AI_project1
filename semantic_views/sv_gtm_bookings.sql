-- =============================================================================
-- GTM Bookings Semantic View (Dimensional Model)
-- Built from fct_bookings + SCD Type 2 dimension tables
-- Supports: Booking metric classifications (NL, NS, Reno, PI),
--           period-over-period (YoY/QoQ/MoM), multi-path joins,
--           and AI/Cortex Agent optimization
-- =============================================================================

-- Set these variables before executing
SET DATABASE = 'PROD_DB';
SET SCHEMA = 'ANALYTICS';

CREATE OR REPLACE SEMANTIC VIEW $DATABASE.$SCHEMA.SV_GTM_BOOKINGS

  -- =========================================================================
  -- TABLES
  -- =========================================================================
  TABLES (
    -- Fact: Bookings (one row per booking = opportunity + solution_total combination)
    bookings AS $DATABASE.$SCHEMA.FCT_BOOKINGS
      PRIMARY KEY (booking_key)
      COMMENT = 'Transactional bookings fact table. Each row represents one closed booking for an opportunity-solution combination. Grain: opportunity_id + solution_total_id. Use booking_or_closed_date for time-based analysis.',

    -- Dimension: Opportunities (SCD Type 2)
    opportunities AS $DATABASE.$SCHEMA.DIM_OPPORTUNITIES
      PRIMARY KEY (opportunity_id)
      CONSTRAINT opp_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 opportunity dimension. Tracks historical changes to opportunity attributes.',

    -- Dimension: Accounts (SCD Type 2)
    accounts AS $DATABASE.$SCHEMA.DIM_ACCOUNTS
      PRIMARY KEY (account_id)
      CONSTRAINT acct_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 account dimension. Tracks historical changes to account attributes.',

    -- Dimension: Users - Opportunity Owner (SCD Type 2)
    opp_owner AS $DATABASE.$SCHEMA.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT owner_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension for the opportunity owner/closer.',

    -- Dimension: Users - Opportunity Creator (SCD Type 2)
    opp_creator AS $DATABASE.$SCHEMA.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT creator_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension for the opportunity creator.',

    -- Dimension: Users - Owner Manager (SCD Type 2)
    owner_manager AS $DATABASE.$SCHEMA.DIM_USERS
      PRIMARY KEY (user_id)
      CONSTRAINT mgr_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 user dimension for the opportunity owner manager.',

    -- Dimension: Opportunity Alignments (SCD Type 2)
    alignments AS $DATABASE.$SCHEMA.DIM_OPPORTUNITY_ALIGNMENTS
      PRIMARY KEY (opportunity_id)
      CONSTRAINT align_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 alignment dimension. Market segment, team, region, sub-region.',

    -- Dimension: Solution Classifications (SCD Type 2)
    solution_classifications AS $DATABASE.$SCHEMA.DIM_OPPORTUNITY_SOLUTION_CLASSIFICATIONS
      PRIMARY KEY (opportunity_solution_classification_key)
      CONSTRAINT solclass_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 solution classification dimension. Solution category, booking category, SKU grouping, persona.',

    -- Dimension: Contacts (SCD Type 2)
    contacts AS $DATABASE.$SCHEMA.DIM_CONTACTS
      PRIMARY KEY (contact_id)
      CONSTRAINT contact_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 contact dimension. Contact name, title, lead source.',

    -- Dimension: Opportunity Partners (SCD Type 2)
    partners AS $DATABASE.$SCHEMA.DIM_OPPORTUNITY_PARTNERS
      PRIMARY KEY (opportunity_id)
      CONSTRAINT partner_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 partner dimension. Up to 8 partners per opportunity.',

    -- Dimension: Opportunity Line Item Summary (SCD Type 2)
    line_items AS $DATABASE.$SCHEMA.DIM_OPPORTUNITY_LINE_ITEM_SUMMARY
      PRIMARY KEY (opportunity_id)
      CONSTRAINT li_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 line item summary. Aggregated product names per opportunity.',

    -- Dimension: Opportunity Assist Summary (SCD Type 2)
    assists AS $DATABASE.$SCHEMA.DIM_OPPORTUNITY_ASSIST_SUMMARY
      PRIMARY KEY (opportunity_id)
      CONSTRAINT assist_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 assist summary. Value management and assist engagement details.',

    -- Dimension: Contracts (SCD Type 2)
    contracts AS $DATABASE.$SCHEMA.DIM_CONTRACTS
      PRIMARY KEY (contract_id)
      CONSTRAINT contract_range DISTINCT RANGE BETWEEN version_start_at AND version_end_at EXCLUSIVE
      COMMENT = 'SCD Type 2 contract dimension. Contract lifecycle information.'
  )

  -- =========================================================================
  -- RELATIONSHIPS
  -- All SCD2 dimension joins use booking_or_closed_date cast to TIMESTAMP_NTZ
  -- as the temporal key from fct_bookings, matched against version_start_at /
  -- version_end_at ranges on each dimension for point-in-time attributes.
  -- =========================================================================
  RELATIONSHIPS (
    -- Bookings -> Opportunities (temporal via booking date)
    bookings_to_opportunities AS
      bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES opportunities(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Accounts (temporal via booking date)
    bookings_to_accounts AS
      bookings(customer_account_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES accounts(account_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Opportunity Owner (temporal, multi-path user)
    bookings_to_owner AS
      bookings(opportunity_owner_user_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES opp_owner(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Opportunity Creator (temporal, multi-path user)
    bookings_to_creator AS
      bookings(opportunity_created_by_user_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES opp_creator(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Solution Classifications (temporal)
    bookings_to_solution_classifications AS
      bookings(opportunity_solution_classification_key, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES solution_classifications(opportunity_solution_classification_key, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Contacts (temporal)
    bookings_to_contacts AS
      bookings(first_associated_contact_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES contacts(contact_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Alignments (temporal via opportunity_id)
    bookings_to_alignments AS
      bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES alignments(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Partners (temporal via opportunity_id)
    bookings_to_partners AS
      bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES partners(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Line Item Summary (temporal via opportunity_id)
    bookings_to_line_items AS
      bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES line_items(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Assist Summary (temporal via opportunity_id)
    bookings_to_assists AS
      bookings(opportunity_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES assists(opportunity_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Bookings -> Contracts (temporal via first_contract_id)
    bookings_to_contracts AS
      bookings(first_contract_id, booking_or_closed_date::TIMESTAMP_NTZ)
        REFERENCES contracts(contract_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE),

    -- Owner -> Owner's Manager (chained from owner)
    owner_to_manager AS
      opp_owner(manager_user_id, version_start_at)
        REFERENCES owner_manager(user_id, BETWEEN version_start_at AND version_end_at EXCLUSIVE)
  )

  -- =========================================================================
  -- FACTS
  -- =========================================================================
  FACTS (
    -- Core booking amount
    bookings.f_first_year_amt AS first_year_s_s_net_amt_usd
      COMMENT = 'First Year Subscription & Support Net Amount in USD for this booking record',

    -- Booking date (used as temporal join key)
    PRIVATE bookings.f_booking_timestamp AS booking_or_closed_date::TIMESTAMP_NTZ
      COMMENT = 'Booking or closed date cast to timestamp for temporal range join compatibility',

    -- Booking status flags
    bookings.f_is_closed_won AS is_closed_won
      COMMENT = 'TRUE if the opportunity stage is Closed Won',

    bookings.f_is_closed_pending AS is_closed_pending
      COMMENT = 'TRUE if the opportunity stage is Closed Pending',

    bookings.f_is_closed_lost AS is_closed_lost
      COMMENT = 'TRUE if the opportunity stage is Closed Lost',

    -- Delivery attributes
    bookings.f_delivery_type AS delivery_type
      COMMENT = 'Delivery type for the solution (e.g. DIY/None, Partner, Workiva Led)',

    bookings.f_is_diy_delivery AS is_diy_delivery
      COMMENT = 'TRUE if the delivery type is DIY/None',

    bookings.f_large_deal_category AS large_deal_category
      COMMENT = 'Large deal size classification category',

    bookings.f_delivery_partner AS delivery_partner
      COMMENT = 'Name of the partner delivering services, if applicable'
  )

  -- =========================================================================
  -- DIMENSIONS
  -- =========================================================================
  DIMENSIONS (
    -- === Time Dimensions (from booking date) ===
    bookings.booking_date_dim AS booking_or_closed_date
      WITH SYNONYMS = ('booking date', 'closed date', 'transaction date')
      COMMENT = 'Primary date for the booking (booking date if available, otherwise closed date). Use for time-based analysis.',

    bookings.booking_year_dim AS YEAR(booking_or_closed_date)
      WITH SYNONYMS = ('year', 'booking year', 'fiscal year')
      COMMENT = 'Calendar year of the booking',

    bookings.booking_quarter_dim AS CONCAT('Q', QUARTER(booking_or_closed_date), ' ', YEAR(booking_or_closed_date))
      WITH SYNONYMS = ('quarter', 'booking quarter', 'fiscal quarter')
      COMMENT = 'Quarter of the booking (e.g. Q1 2025)',

    bookings.booking_month_dim AS TO_VARCHAR(booking_or_closed_date, 'YYYY-MM')
      WITH SYNONYMS = ('month', 'booking month')
      COMMENT = 'Year-month of the booking (e.g. 2025-03)',

    bookings.booking_week_dim AS TO_VARCHAR(DATE_TRUNC('WEEK', booking_or_closed_date), 'YYYY-MM-DD')
      WITH SYNONYMS = ('week', 'booking week')
      COMMENT = 'Week start date of the booking',

    -- === Booking Identifier Dimensions ===
    bookings.booking_key_dim AS booking_key
      COMMENT = 'Unique surrogate key for each booking record (opportunity_id + solution_total_id)',

    bookings.opportunity_id_dim AS opportunity_id
      WITH SYNONYMS = ('opp id', 'deal id')
      COMMENT = 'Salesforce opportunity identifier',

    bookings.currency_iso_code_dim AS currency_iso_code
      COMMENT = 'Original currency ISO code of the booking',

    -- === Booking Status Dimensions ===
    bookings.is_closed_won_dim AS is_closed_won
      WITH SYNONYMS = ('won', 'closed won')
      COMMENT = 'TRUE if the opportunity is Closed Won',

    bookings.is_closed_pending_dim AS is_closed_pending
      WITH SYNONYMS = ('pending', 'closed pending')
      COMMENT = 'TRUE if the opportunity is Closed Pending',

    bookings.is_closed_lost_dim AS is_closed_lost
      WITH SYNONYMS = ('lost', 'closed lost')
      COMMENT = 'TRUE if the opportunity is Closed Lost',

    bookings.delivery_type_dim AS delivery_type
      COMMENT = 'Delivery type (e.g. DIY/None, Partner, Workiva Led)',

    bookings.is_diy_delivery_dim AS is_diy_delivery
      COMMENT = 'TRUE if delivery type is DIY/None',

    bookings.large_deal_category_dim AS large_deal_category
      COMMENT = 'Large deal size classification',

    bookings.solution_total_name_dim AS solution_total_name
      COMMENT = 'Name of the solution total record',

    bookings.opportunity_line_type_dim AS opportunity_line_type
      COMMENT = 'Line type classification of the booking',

    -- === Opportunity Dimensions (from dim_opportunities via temporal join) ===
    opportunities.opportunity_name_dim AS opportunity_name
      WITH SYNONYMS = ('opp name', 'deal name')
      COMMENT = 'Name of the opportunity as of the booking date',

    opportunities.opportunity_type_dim AS opportunity_type
      WITH SYNONYMS = ('opp type', 'deal type')
      COMMENT = 'Type of opportunity (e.g. New Deal, Renewal, Price Increase)',

    opportunities.opportunity_deal_type_dim AS opportunity_deal_type
      COMMENT = 'High-level deal classification (e.g. SaaS, Consulting, Government)',

    opportunities.opportunity_reporting_sub_type_dim AS opportunity_reporting_sub_type
      COMMENT = 'Reporting sub-type (e.g. New Deal, Renewal)',

    opportunities.opportunity_record_type_name_dim AS opportunity_record_type_name
      WITH SYNONYMS = ('record type')
      COMMENT = 'Salesforce record type (e.g. Direct, New Deal, Price Increase)',

    opportunities.opportunity_stage_name_dim AS opportunity_stage_name
      WITH SYNONYMS = ('stage', 'opp stage')
      COMMENT = 'Stage of the opportunity at the time of booking',

    opportunities.opportunity_forecast_category_name_dim AS opportunity_forecast_category_name
      WITH SYNONYMS = ('forecast category')
      COMMENT = 'Forecast category (e.g. Commit, Upside, Best Case)',

    opportunities.opportunity_closed_date_dim AS opportunity_closed_date
      WITH SYNONYMS = ('close date')
      COMMENT = 'Date the opportunity was closed',

    opportunities.opportunity_created_date_dim AS opportunity_created_date
      WITH SYNONYMS = ('created date', 'opp created date')
      COMMENT = 'Date the opportunity was created',

    opportunities.opportunity_qualified_date_dim AS opportunity_qualified_date
      WITH SYNONYMS = ('qualified date')
      COMMENT = 'Date the opportunity first reached a qualified stage',

    opportunities.opportunity_bookings_date_dim AS opportunity_bookings_date
      WITH SYNONYMS = ('bookings date')
      COMMENT = 'Date the revenue was booked',

    opportunities.opportunity_booking_or_closed_date_dim AS opportunity_booking_or_closed_date
      WITH SYNONYMS = ('booking or closed date')
      COMMENT = 'Booking date if available, otherwise closed date (from opportunity dimension)',

    opportunities.opportunity_origin_source_dim AS opportunity_origin_source
      WITH SYNONYMS = ('origin source', 'lead source')
      COMMENT = 'Origin source of the opportunity (e.g. Organic, MCL)',

    opportunities.is_deal_registration_dim AS is_deal_registration
      COMMENT = 'TRUE if this opportunity is a deal registration',

    opportunities.is_alignment_stage_plus_dim AS is_alignment_stage_plus
      COMMENT = 'TRUE if the opportunity is at or past alignment stage',

    -- === Account Dimensions (from dim_accounts via temporal join) ===
    accounts.account_name_dim AS account_name
      WITH SYNONYMS = ('company name', 'customer name', 'company')
      COMMENT = 'Name of the customer account as of the booking date',

    accounts.account_region_dim AS account_region
      WITH SYNONYMS = ('account geo', 'customer region')
      COMMENT = 'Geographic region of the account (e.g. North America, EMEA, APAC, LATAM)',

    accounts.account_segment_dim AS account_segment
      WITH SYNONYMS = ('account segment', 'customer segment')
      COMMENT = 'Business segment of the account',

    accounts.account_type_dim AS account_type
      COMMENT = 'Account type (e.g. Publicly Traded Company, Private)',

    accounts.account_corporate_country_dim AS account_corporate_country
      WITH SYNONYMS = ('country', 'customer country')
      COMMENT = 'Country of the account corporation',

    accounts.account_workiva_internal_industry_dim AS account_workiva_internal_industry
      WITH SYNONYMS = ('industry', 'internal industry')
      COMMENT = 'Workiva internal industry classification',

    accounts.account_industry_group_dim AS account_industry_group
      COMMENT = 'Industry group classification of the account',

    accounts.account_owner_role_dim AS account_owner_role
      COMMENT = 'Role of the account owner',

    accounts.is_financial_industry_dim AS is_financial_industry
      COMMENT = 'TRUE if the account is in the financial industry',

    accounts.is_energy_industry_dim AS is_energy_industry
      COMMENT = 'TRUE if the account is in the energy industry',

    accounts.financial_industry_type_dim AS financial_industry_type
      COMMENT = 'Type of financial industry (e.g. Insurance, Banking)',

    accounts.energy_industry_type_dim AS energy_industry_type
      COMMENT = 'Type of energy industry (e.g. Oil & Gas, Utilities)',

    accounts.is_test_or_internal_account_dim AS is_test_or_internal_account
      COMMENT = 'TRUE if test or internal account. Filter to FALSE to exclude test data.',

    accounts.is_partner_dim AS is_partner_account
      COMMENT = 'TRUE if the account is a partner',

    accounts.is_customer_dim AS is_customer
      COMMENT = 'TRUE if the account is a current customer',

    accounts.is_on_the_path_dim AS is_on_the_path
      COMMENT = 'TRUE if the account is on the path (related to onboarding/sales path)',

    accounts.account_csrd_dim AS account_csrd
      COMMENT = 'CSRD classification of the account',

    accounts.account_wk_company_type_dim AS account_wk_company_type
      COMMENT = 'Workiva company type classification',

    -- Ultimate Parent Account dimensions
    accounts.ultimate_parent_account_name_dim AS ultimate_parent_account_name
      WITH SYNONYMS = ('parent account', 'parent company', 'ultimate parent')
      COMMENT = 'Name of the ultimate parent account',

    accounts.ultimate_parent_account_id_18_digit_dim AS ultimate_parent_account_id_18_digit
      COMMENT = '18-digit ID of the ultimate parent account',

    accounts.ultimate_parent_account_type_dim AS ultimate_parent_account_type
      COMMENT = 'Account type of the ultimate parent',

    accounts.ultimate_parent_region_dim AS ultimate_parent_region
      COMMENT = 'Region of the ultimate parent',

    accounts.ultimate_parent_segment_dim AS ultimate_parent_segment
      COMMENT = 'Segment of the ultimate parent',

    accounts.ultimate_parent_corporate_country_dim AS ultimate_parent_corporate_country
      COMMENT = 'Corporate country of the ultimate parent',

    accounts.ultimate_parent_csrd_dim AS ultimate_parent_csrd
      COMMENT = 'CSRD classification of the ultimate parent',

    accounts.ultimate_parent_customer_flag_dim AS ultimate_parent_customer_flag
      COMMENT = 'TRUE if the ultimate parent is a customer',

    accounts.ultimate_parent_industry_group_dim AS ultimate_parent_industry_group
      COMMENT = 'Industry group of the ultimate parent',

    accounts.ultimate_parent_wk_company_type_dim AS ultimate_parent_wk_company_type
      COMMENT = 'Workiva company type of the ultimate parent',

    accounts.ultimate_parent_account_owner_role_dim AS ultimate_parent_account_owner_role
      COMMENT = 'Account owner role of the ultimate parent',

    -- === Alignment Dimensions (from dim_opportunity_alignments via temporal join) ===
    alignments.opportunity_market_segment_dim AS opportunity_market_segment
      WITH SYNONYMS = ('market segment', 'segment', 'sales segment')
      COMMENT = 'Sales segment aligned to the opportunity',

    alignments.opportunity_team_dim AS opportunity_team
      WITH SYNONYMS = ('team', 'sales team')
      COMMENT = 'Sales team aligned to the opportunity',

    alignments.opportunity_owner_region_dim AS opportunity_owner_region
      WITH SYNONYMS = ('owner region', 'sales region', 'region')
      COMMENT = 'Region aligned to the opportunity',

    alignments.opportunity_owner_sub_region_dim AS opportunity_owner_sub_region
      WITH SYNONYMS = ('sub region', 'sub-region', 'territory')
      COMMENT = 'Sub-region aligned to the opportunity',

    alignments.public_type_dim AS public_type
      COMMENT = 'Public sector type (SLED or Federal) if applicable',

    alignments.is_public_type_dim AS is_public_type
      COMMENT = 'TRUE if the opportunity team is public sector (SLED or Federal)',

    alignments.opportunity_alignment_category_dim AS opportunity_alignment_category
      COMMENT = 'Aggregated alignment category (e.g. North America, EMEA, Global MSP)',

    alignments.opportunity_alignment_sub_category_dim AS opportunity_alignment_sub_category
      COMMENT = 'Alignment sub-category derived from market segment',

    alignments.opportunity_alignment_region_dim AS opportunity_alignment_region
      COMMENT = 'Alignment region derived from category and market segment',

    -- === Solution Classification Dimensions (via temporal join) ===
    solution_classifications.solution_category_dim AS solution_category
      WITH SYNONYMS = ('product category', 'solution')
      COMMENT = 'Category of the solution (e.g. SEC/SEDAR, ESG, Controls Management)',

    solution_classifications.booking_category_dim AS booking_category
      WITH SYNONYMS = ('booking type')
      COMMENT = 'Booking category (e.g. Management Reporting, Sustainability, Risk)',

    solution_classifications.solution_group_dim AS solution_group
      WITH SYNONYMS = ('product group')
      COMMENT = 'Solution group (e.g. Financial Reporting, GRC, Sustainability)',

    solution_classifications.sku_grouping_dim AS sku_grouping
      WITH SYNONYMS = ('product grouping', 'sku group')
      COMMENT = 'SKU grouping of the solution',

    solution_classifications.solution_picklist_dim AS solution_picklist
      COMMENT = 'Solution picklist value from Salesforce',

    solution_classifications.persona_dim AS persona
      WITH SYNONYMS = ('target persona', 'buyer persona')
      COMMENT = 'Business persona targeted (e.g. Accounting & Finance, Sustainability)',

    solution_classifications.reporting_classification_bookings_dim AS reporting_classification
      WITH SYNONYMS = ('reporting category', 'classification')
      COMMENT = 'Reporting classification for bookings: NL (New Logo), NS (New Solution), Reno (Renegotiation), PI-Comm Ops (Price Increase), Churn',

    solution_classifications.is_multi_category_dim AS is_multi_category
      COMMENT = 'TRUE if the opportunity spans multiple solution groups',

    solution_classifications.is_multi_solution_category_dim AS is_multi_solution_category
      COMMENT = 'TRUE if the opportunity has multiple distinct solution categories',

    solution_classifications.is_advanced_sku_grouping_dim AS is_advanced_sku_grouping
      COMMENT = 'TRUE if the opportunity has Advanced SKU grouping',

    solution_classifications.is_partner_delivered_dim AS is_partner_delivered
      COMMENT = 'TRUE if the solution is partner-delivered',

    -- === Opportunity Owner Dimensions (multi-path via bookings_to_owner) ===
    opp_owner.user_name_dim AS opportunity_owner_name
      WITH SYNONYMS = ('rep name', 'owner name', 'sales rep', 'closer name')
      COMMENT = 'Name of the opportunity owner/closer as of the booking date',

    opp_owner.user_role_dim AS opportunity_owner_role
      WITH SYNONYMS = ('owner role', 'rep role')
      COMMENT = 'Role of the opportunity owner',

    opp_owner.user_department_dim AS opportunity_owner_department
      COMMENT = 'Department of the opportunity owner',

    opp_owner.user_division_dim AS opportunity_owner_division
      COMMENT = 'Division of the opportunity owner',

    -- === Opportunity Creator Dimensions (multi-path via bookings_to_creator) ===
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
      COMMENT = 'Name of the primary partner',

    partners.partner_1_relationship_dim AS partner_1_relationship
      COMMENT = 'Relationship type of the primary partner',

    partners.partner_1_type_dim AS partner_1_type
      COMMENT = 'Type of the primary partner',

    partners.partner_1_account_region_dim AS partner_1_region
      COMMENT = 'Region of the primary partner',

    -- === Line Item Dimensions ===
    line_items.opportunity_products_dim AS opportunity_products
      WITH SYNONYMS = ('products', 'product list')
      COMMENT = 'Comma-separated list of product names on this opportunity',

    -- === Assist Dimensions ===
    assists.has_completed_value_management_assist_dim AS has_completed_value_management_assist
      COMMENT = 'TRUE if a value management assist was completed for this opportunity',

    assists.latest_vm_assist_value_stage_dim AS latest_vm_assist_value_stage
      COMMENT = 'Value stage of the latest value management assist'
  )

  -- =========================================================================
  -- METRICS
  -- =========================================================================
  METRICS (
    -- ===== Core Booking Metrics =====

    bookings.total_bookings AS SUM(first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('bookings value', 'sum of bookings', 'total booking amount', 'absolute dollars')
      COMMENT = 'Total first year S&S net amount (USD) across all booking records. Includes all booking types (NL, NS, Reno, PI, Churn).',

    bookings.booking_count AS COUNT(DISTINCT booking_key)
      WITH SYNONYMS = ('bookings volume', 'total volume', 'volume', 'number of bookings')
      COMMENT = 'Count of distinct booking records',

    bookings.opportunity_count AS COUNT(DISTINCT opportunity_id)
      WITH SYNONYMS = ('deal count', 'opp count', 'number of opportunities')
      COMMENT = 'Count of distinct opportunities with bookings',

    bookings.account_count AS COUNT(DISTINCT customer_account_id)
      WITH SYNONYMS = ('customer count', 'number of accounts')
      COMMENT = 'Count of distinct accounts with bookings',

    bookings.avg_booking_amount AS AVG(first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('average deal size', 'avg booking value')
      COMMENT = 'Average first year S&S net amount per booking record',

    bookings.max_booking_amount AS MAX(first_year_s_s_net_amt_usd)
      COMMENT = 'Largest single booking amount (USD)',

    -- ===== Booking Classification Metrics (using stage-based flags from fct_bookings) =====
    -- Note: is_metric_booking, is_metric_new_logo_booking, etc. are computed in the OBT
    -- via dbt macros. In the dimensional model, we use the closed status flags from fct_bookings
    -- combined with reporting_classification from dim_opportunity_solution_classifications.

    bookings.closed_won_bookings AS SUM(CASE WHEN is_closed_won THEN first_year_s_s_net_amt_usd ELSE 0 END)
      WITH SYNONYMS = ('won bookings', 'closed won amount')
      COMMENT = 'Total booking amount for closed won opportunities',

    bookings.closed_won_count AS COUNT(DISTINCT CASE WHEN is_closed_won THEN booking_key END)
      WITH SYNONYMS = ('won count', 'closed won volume')
      COMMENT = 'Count of closed won bookings',

    bookings.closed_pending_bookings AS SUM(CASE WHEN is_closed_pending THEN first_year_s_s_net_amt_usd ELSE 0 END)
      WITH SYNONYMS = ('pending bookings', 'closed pending amount')
      COMMENT = 'Total booking amount for closed pending opportunities',

    bookings.closed_pending_count AS COUNT(DISTINCT CASE WHEN is_closed_pending THEN booking_key END)
      WITH SYNONYMS = ('pending count')
      COMMENT = 'Count of closed pending bookings',

    -- ===== Multi-Path Metrics (account path) =====

    bookings.total_bookings_by_account
      USING (bookings_to_accounts)
      AS SUM(first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('bookings by account', 'account bookings')
      COMMENT = 'Total bookings resolved via accounts path. Use when grouping by account dimensions.',

    bookings.booking_count_by_account
      USING (bookings_to_accounts)
      AS COUNT(DISTINCT booking_key)
      COMMENT = 'Booking count resolved via accounts path',

    bookings.account_count_by_account
      USING (bookings_to_accounts)
      AS COUNT(DISTINCT customer_account_id)
      COMMENT = 'Account count resolved via accounts path',

    -- ===== Multi-Path Metrics (owner user path) =====

    bookings.total_bookings_by_owner
      USING (bookings_to_owner)
      AS SUM(first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('bookings by rep', 'rep bookings', 'owner bookings')
      COMMENT = 'Total bookings resolved via opportunity owner. Use when grouping by owner/rep dimensions.',

    bookings.booking_count_by_owner
      USING (bookings_to_owner)
      AS COUNT(DISTINCT booking_key)
      WITH SYNONYMS = ('volume by rep')
      COMMENT = 'Booking count resolved via opportunity owner',

    -- ===== Multi-Path Metrics (alignment path) =====

    bookings.total_bookings_by_alignment
      USING (bookings_to_alignments)
      AS SUM(first_year_s_s_net_amt_usd)
      WITH SYNONYMS = ('bookings by segment', 'segment bookings')
      COMMENT = 'Total bookings resolved via alignments. Use when grouping by segment, team, or region.',

    bookings.booking_count_by_alignment
      USING (bookings_to_alignments)
      AS COUNT(DISTINCT booking_key)
      COMMENT = 'Booking count resolved via alignments',

    -- ===== Window Metrics: Year-over-Year (YoY) =====

    bookings.total_bookings_yoy AS LAG(bookings.total_bookings, 365)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_year_dim
            ORDER BY bookings.booking_date_dim)
      WITH SYNONYMS = ('bookings same day last year', 'prior year bookings', 'yoy bookings')
      COMMENT = 'Total bookings from the same day one year ago. Requires booking_date_dim and booking_year_dim in query.',

    bookings.booking_count_yoy AS LAG(bookings.booking_count, 365)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_year_dim
            ORDER BY bookings.booking_date_dim)
      WITH SYNONYMS = ('volume last year')
      COMMENT = 'Booking count from the same day one year ago',

    -- ===== Window Metrics: Quarter-over-Quarter (QoQ) =====

    bookings.total_bookings_qoq AS LAG(bookings.total_bookings, 91)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_quarter_dim
            ORDER BY bookings.booking_date_dim)
      WITH SYNONYMS = ('bookings last quarter', 'prior quarter bookings', 'qoq bookings')
      COMMENT = 'Total bookings from approximately one quarter ago (91 days). Requires booking_date_dim and booking_quarter_dim in query.',

    -- ===== Window Metrics: Month-over-Month (MoM) =====

    bookings.total_bookings_mom AS LAG(bookings.total_bookings, 30)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_month_dim
            ORDER BY bookings.booking_date_dim)
      WITH SYNONYMS = ('bookings last month', 'prior month bookings', 'mom bookings')
      COMMENT = 'Total bookings from approximately one month ago (30 days). Requires booking_date_dim and booking_month_dim in query.',

    -- ===== Window Metrics: Rolling Averages =====

    bookings.total_bookings_7d_avg AS AVG(bookings.total_bookings)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_year_dim
            ORDER BY bookings.booking_date_dim
            RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW)
      WITH SYNONYMS = ('7-day average bookings', 'rolling weekly bookings')
      COMMENT = 'Rolling 7-day average of total bookings',

    bookings.total_bookings_30d_avg AS AVG(bookings.total_bookings)
      OVER (PARTITION BY EXCLUDING bookings.booking_date_dim, bookings.booking_month_dim
            ORDER BY bookings.booking_date_dim
            RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW)
      WITH SYNONYMS = ('30-day average bookings', 'rolling monthly bookings')
      COMMENT = 'Rolling 30-day average of total bookings',

    -- ===== Derived Metrics (view-level) =====

    avg_bookings_per_account AS
      DIV0(bookings.total_bookings, bookings.account_count)
      WITH SYNONYMS = ('bookings per account', 'account average bookings')
      COMMENT = 'Average booking amount per distinct account',

    avg_bookings_per_opportunity AS
      DIV0(bookings.total_bookings, bookings.opportunity_count)
      WITH SYNONYMS = ('bookings per opportunity', 'average deal size metric')
      COMMENT = 'Average booking amount per distinct opportunity',

    bookings_yoy_change AS
      bookings.total_bookings - bookings.total_bookings_yoy
      WITH SYNONYMS = ('yoy bookings change', 'year over year change')
      COMMENT = 'Absolute change in total bookings compared to same day last year',

    bookings_yoy_pct_change AS
      DIV0(bookings.total_bookings - bookings.total_bookings_yoy, bookings.total_bookings_yoy) * 100
      WITH SYNONYMS = ('yoy bookings growth', 'year over year percent change')
      COMMENT = 'Percentage change in total bookings compared to same day last year',

    bookings_qoq_change AS
      bookings.total_bookings - bookings.total_bookings_qoq
      WITH SYNONYMS = ('qoq bookings change', 'quarter over quarter change')
      COMMENT = 'Absolute change in total bookings compared to approximately one quarter ago',

    bookings_qoq_pct_change AS
      DIV0(bookings.total_bookings - bookings.total_bookings_qoq, bookings.total_bookings_qoq) * 100
      WITH SYNONYMS = ('qoq bookings growth')
      COMMENT = 'Percentage change in total bookings compared to approximately one quarter ago',

    bookings_mom_change AS
      bookings.total_bookings - bookings.total_bookings_mom
      WITH SYNONYMS = ('mom bookings change', 'month over month change')
      COMMENT = 'Absolute change in total bookings compared to approximately one month ago',

    bookings_mom_pct_change AS
      DIV0(bookings.total_bookings - bookings.total_bookings_mom, bookings.total_bookings_mom) * 100
      WITH SYNONYMS = ('mom bookings growth')
      COMMENT = 'Percentage change in total bookings compared to approximately one month ago'
  )

  COMMENT = 'Workiva GTM Bookings Analytics — built on the dimensional model (fct_bookings + SCD Type 2 dimensions). Tracks closed bookings with historical account, opportunity, alignment, solution, contact, partner, and contract attributes resolved via temporal range joins. Supports booking classification analysis (NL, NS, Reno, PI), multi-path metrics, and YoY/QoQ/MoM period-over-period comparisons.'

  AI_SQL_GENERATION '
    This semantic view models Workiva Go-To-Market bookings data built on a dimensional model.
    The fact table (fct_bookings) is transactional — each row represents one closed booking for
    an opportunity-solution combination. Unlike the pipeline semantic view (which uses daily snapshots),
    bookings are point-in-time events and do NOT require semi-additive metrics.

    All dimension tables are SCD Type 2 and joined via temporal range joins using
    booking_or_closed_date (cast to TIMESTAMP_NTZ), so dimension values are resolved to
    the version that was current at the time of booking.

    CRITICAL RULES FOR SQL GENERATION:

    1. BOOKING CLASSIFICATIONS: Bookings are classified by reporting_classification dimension:
       - NL (New Logo): First-time customer bookings
       - NS (New Solution): Existing customer buying a new solution category
       - Reno (Renegotiation): Contract renegotiations
       - PI-Comm Ops (Price Increase): Price increase bookings
       - Churn: Customer churn bookings (typically negative amounts)

       To filter for specific booking types, use the reporting_classification dimension.
       Example: WHERE reporting_classification = ''NL'' for New Logo bookings.

    2. METRIC BOOKINGS: The concept of "is_metric_booking" from the OBT means bookings where
       the opportunity stage is Closed Won or Closed Pending AND the booking is not churned.
       In the dimensional model, filter using:
       - is_closed_won = TRUE OR is_closed_pending = TRUE (for metric bookings)
       - reporting_classification != ''Churn'' (to exclude churn)

    3. NON-PI BOOKINGS: A frequently requested metric. Filter using:
       reporting_classification != ''PI-Comm Ops'' AND (is_closed_won = TRUE OR is_closed_pending = TRUE)

    4. BOOKING AMOUNTS: first_year_s_s_net_amt_usd is the primary booking amount in USD.
       This can be negative (e.g., for churn or downsells). Users refer to amounts in
       thousands or millions (e.g. "$500K NL bookings").

    5. TIME-BASED ANALYSIS: Use booking_date_dim (booking_or_closed_date) as the primary
       time dimension. This is the booking date if available, otherwise the closed date.

    6. PERIOD-OVER-PERIOD: For YoY use total_bookings_yoy and bookings_yoy_pct_change.
       For QoQ use total_bookings_qoq and bookings_qoq_pct_change.
       For MoM use total_bookings_mom and bookings_mom_pct_change.
       These window metrics REQUIRE booking_date_dim in the query dimensions.

    7. MULTI-PATH RESOLUTION: When grouping by account dimensions, use total_bookings_by_account.
       When grouping by owner/rep dimensions, use total_bookings_by_owner.
       When grouping by segment/region/team, use total_bookings_by_alignment.

    8. TEST ACCOUNTS: Filter out test/internal accounts with is_test_or_internal_account = FALSE.

    9. SALES ALIGNMENT vs ACCOUNT GEO: "Segment", "region", "sub_region", and "team" refer
       to sales alignment (from dim_opportunity_alignments). Use account_region for account
       geographic region. Use opportunity_owner_region for sales region.

    10. WIN RATE: To calculate win rate from this view, compare closed_won_count vs total
        booking_count where is_closed_won = TRUE or is_closed_lost = TRUE.

    11. CYCLE TIME: Calculate as DATEDIFF(day, opportunity_qualified_date, booking_date_dim)
        for closed won bookings.

    12. QUARTER FORMAT: booking_quarter_dim uses format like "Q1 2025".

    13. COMMON QUERIES:
        - "Total NL bookings this quarter": Filter reporting_classification = ''NL'',
          booking_quarter = current quarter, and (is_closed_won OR is_closed_pending).
        - "Bookings by account region": Use total_bookings_by_account grouped by account_region.
        - "YoY bookings growth": Use bookings_yoy_pct_change with booking_date_dim.
  '

  AI_QUESTION_CATEGORIZATION '
    Classify questions into these categories:
    - BOOKINGS_TOTAL: Total bookings amounts, volumes, averages across all types
    - BOOKINGS_BY_TYPE: Bookings by classification (NL, NS, Reno, PI, Churn, Non-PI)
    - BOOKINGS_TRENDS: Period-over-period changes, YoY/QoQ/MoM growth rates, rolling averages
    - BOOKINGS_BY_SEGMENT: Bookings by market segment, team, region, sub-region (sales alignment)
    - BOOKINGS_BY_ACCOUNT: Bookings by account name, account region, industry, parent account
    - BOOKINGS_BY_PRODUCT: Bookings by solution category, solution group, SKU grouping, persona
    - BOOKINGS_BY_REP: Bookings by opportunity owner, creator, manager
    - BOOKINGS_BY_STAGE: Bookings by opportunity stage, deal type, record type
    - WIN_RATE_ANALYSIS: Win rates, close rates, cycle time analysis
    - PARTNER_ANALYSIS: Bookings involving partners
    - OPPORTUNITY_DETAIL: Questions about specific bookings or opportunities
    - OUT_OF_SCOPE: Questions about pipeline, forecasting, or topics not in bookings data.
      Redirect to the GTM Pipeline semantic view for pipeline-related questions.
  '
;

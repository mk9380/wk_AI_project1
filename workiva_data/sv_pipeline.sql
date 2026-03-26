{{ config(materialized='semantic_view') }}

{% set obt_pipeline = ref("obt_pipeline") %}

name: {{ this.identifier }}
description: This semantic view contains sales pipeline information.
tables:
  - name: OBT_PIPELINE
    description: A OBT (One Big Table) that combines daily snapshot data from ''fct_pipeline'' with historical attributes from various dimension tables, including opportunities, accounts, alignments, solution classifications, contacts, and users. This model serves as a comprehensive, denormalized source for pipeline analysis and reporting.
    base_table:
      database: {{ obt_pipeline.database }}
      schema: {{ obt_pipeline.schema }}
      table: {{ obt_pipeline.identifier }}
    dimensions:
      - name: ACCOUNT_REGION
        description: The geographic region of the account.
        expr: ACCOUNT_REGION
        data_type: VARCHAR
        sample_values:
          - North America
          - APAC
          - LATAM
      - name: BOOKING_CATEGORY
        description: The standardized booking category for the solution.
        expr: BOOKING_CATEGORY
        data_type: VARCHAR
        sample_values:
          - Management Reporting
          - Sustainability
      - name: ENERGY_INDUSTRY_TYPE
        description: A derived field classifying the type of energy industry.
        expr: ENERGY_INDUSTRY_TYPE
        data_type: VARCHAR
        sample_values:
          - Oil & Gas
          - Utilities
      - name: FINANCIAL_INDUSTRY_TYPE
        description: A derived field classifying the type of financial industry.
        expr: FINANCIAL_INDUSTRY_TYPE
        data_type: VARCHAR
        sample_values:
          - Insurance
          - Banking
          - Investments
      - name: IS_CLOSED_LOST
        description: A boolean flag indicating if the opportunity is in a closed lost stage at the corresponding snapshot_date.
        expr: IS_CLOSED_LOST
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_CLOSED_PENDING
        description: A boolean flag indicating if the opportunity is in a closed pending stage at the corresponding snapshot_date.
        expr: IS_CLOSED_PENDING
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_CLOSED_WON
        description: A boolean flag indicating if the opportunity is in a closed won stage at the corresponding snapshot_date.
        expr: IS_CLOSED_WON
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_CURRENT_DAY_SNAPSHOT
        description: A boolean flag indicating if the `snapshot_date` is the current calendar day.
        expr: IS_CURRENT_DAY_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_ENERGY_INDUSTRY
        description: Boolean flag indicating if the account is in the energy industry.
        expr: IS_ENERGY_INDUSTRY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_FIFTH_BUSINESS_DAY_OF_MONTH_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is the fifth business day of its respective month.
        expr: IS_FIFTH_BUSINESS_DAY_OF_MONTH_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_FIFTH_BUSINESS_DAY_OF_QUARTER_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is the fifth business day of its respective quarter.
        expr: IS_FIFTH_BUSINESS_DAY_OF_QUARTER_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_FINANCIAL_INDUSTRY
        description: Boolean flag indicating if the account is in the financial industry.
        expr: IS_FINANCIAL_INDUSTRY
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_FIRST_DAY_OF_MONTH_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is the first day of its respective month.
        expr: IS_FIRST_DAY_OF_MONTH_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_FIRST_DAY_OF_QUARTER_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is the first day of its respective quarter.
        expr: IS_FIRST_DAY_OF_QUARTER_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_MONTHLY_SNAPSHOT
        description: A flag to identify key dates for monthly pipeline reporting (fifth business day of month or current day, excluding future dates within the current month).
        expr: IS_MONTHLY_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_MULTI_SOLUTION_CATEGORY
        description: Boolean flag. TRUE if the Opportunity contains more than one distinct solution_category (excluding Other) that contributes to first year net amount.
        expr: IS_MULTI_SOLUTION_CATEGORY
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_MULTI_CATEGORY
        description: Boolean flag. TRUE if the Opportunity contains more than one distinct solution_group among 'Financial Reporting', 'GRC', and 'Sustainability' that contributes to first year net amount.
        expr: IS_MULTI_CATEGORY
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_ONE_YEAR_AGO_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is exactly one year prior to the current day.
        expr: IS_ONE_YEAR_AGO_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_OPEN
        description: A boolean flag indicating if the opportunity is in an open stage at the corresponding snapshot_date.
        expr: IS_OPEN
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_PARTNER_RELATED
        description: Boolean flag indicating if the opportunity is related to any partner (derived from the existence of `partner_1_account_id`).
        expr: IS_PARTNER_RELATED
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_PUBLIC_TYPE
        description: A boolean flag indicating if the `opportunity_team` is designated as a Public Sector type ('SLED' or 'Federal').
        expr: IS_PUBLIC_TYPE
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_QUALIFIED
        description: A boolean flag indicating if the opportunity is in a qualified stage.
        expr: IS_QUALIFIED
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_QUARTERLY_SNAPSHOT
        description: A flag to identify key dates for quarterly pipeline reporting (fifth business day of quarter or current day, excluding future dates within the current quarter).
        expr: IS_QUARTERLY_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_SNAPSHOT_NEAR_CLOSED_DATE
        description: "Boolean flag (TRUE/FALSE) indicating if the opportunity's snapshot date falls within 1 year before and 2 years after its closed date."
        expr: IS_SNAPSHOT_NEAR_CLOSED_DATE
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_THREE_YEARS_AGO_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is exactly three years prior to the current day.
        expr: IS_THREE_YEARS_AGO_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_TWO_YEARS_AGO_SNAPSHOT
        description: A boolean flag indicating if `snapshot_date` is exactly two years prior to the current day.
        expr: IS_TWO_YEARS_AGO_SNAPSHOT
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: OPPORTUNITY_FORECAST_CATEGORY_NAME
        description: The category used for sales forecasting (e.g., 'Upside', 'Commit').
        expr: OPPORTUNITY_FORECAST_CATEGORY_NAME
        data_type: VARCHAR
        sample_values:
          - Upside
          - Commit
      - name: OPPORTUNITY_ID
        description: The unique identifier for the Salesforce Opportunity.
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        sample_values:
          - 0066f000016kSGtAAM
          - 006PI000002UJC9YAO
          - 0066f00001E0kpVAAR
      - name: OPPORTUNITY_OWNER_REGION
        description: The region aligned to the Opportunity. The value is derived from the Opportunity Closer/Owner's region, but falls back to the Account's region for 'Managed Service / Partner Direct' teams or if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_OWNER_REGION
        data_type: VARCHAR
        sample_values:
          - LATAM
          - EMEA
      - name: OPPORTUNITY_MARKET_SEGMENT
        description: The segment aligned to the Opportunity. The value is derived primarily from the Opportunity Closer/Owner's segment, but falls back to the Account Owner's segment if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_MARKET_SEGMENT
        data_type: VARCHAR
        sample_values:
          - Canada
          - EMEA - North & FSI
          - Strategic FS
      - name: OPPORTUNITY_STAGE_NAME
        description: The stage of the Opportunity (e.g., 'Discovery', 'Negotiate and Close', 'Closed Won').
        expr: OPPORTUNITY_STAGE_NAME
        data_type: VARCHAR
        sample_values:
          - Alignment
          - Propose
          - Define Solution
      - name: OPPORTUNITY_OWNER_SUB_REGION
        description: "The sub-region aligned to the Opportunity. The value is derived from the Opportunity Closer/Owner's sub-region, but falls back to the Account's region for 'Managed Service / Partner Direct' teams, and to the Account's sub-region if `is_account_alignment` is TRUE."
        expr: OPPORTUNITY_OWNER_SUB_REGION
        data_type: VARCHAR
        sample_values:
          - North America
          - LATAM
          - EMEA-DACH
      - name: OPPORTUNITY_TEAM
        description: "The team aligned to the Opportunity. The value is derived primarily from the Opportunity Closer/Owner's team, but falls back to the Account Owner's team if `is_account_alignment` is TRUE."
        expr: OPPORTUNITY_TEAM
        data_type: VARCHAR
        sample_values:
          - Corporate Northeast
          - EMEA - Nordics
          - SLED
      - name: OPPORTUNITY_TYPE
        description: The general classification of the Opportunity type.
        expr: OPPORTUNITY_TYPE
        data_type: VARCHAR
        sample_values:
          - Renegotiation
          - Managed Services
          - New Solution
      - name: PERSONA
        description: The business persona targeted by the solution, derived from the solution classification map.
        expr: PERSONA
        data_type: VARCHAR
        sample_values:
          - Accounting & Finance
          - Sustainability
      - name: PIPELINE_KEY
        description: A unique surrogate key for each pipeline snapshot, generated from the snapshot date, opportunity ID, and solution total ID. This ID represents the primary key.
        expr: PIPELINE_KEY
        data_type: VARCHAR
        sample_values:
          - a8823ba987ce5236fec5ec990958de66
          - 35c4ed4d835adfb85870acd25aa43bda
          - 3db92a5fecc27b8cc286890b8ebe2644
      - name: OPPORTUNITY_PRODUCTS
        description: A comma-separated list of all distinct product names associated with this opportunity.
        expr: OPPORTUNITY_PRODUCTS
        data_type: VARCHAR
        sample_values:
          - Wdesk Connected Financial Reporting for Government - Per Unit
          - ESRS XBRL Reporting
          - Controls Management Essentials
      - name: PUBLIC_TYPE
        description: The public sector type (e.g., 'SLED', 'Federal') if the `opportunity_team` belongs to a public sector designation, otherwise NULL.
        expr: PUBLIC_TYPE
        data_type: VARCHAR
        sample_values:
          - SLED
          - Federal
      - name: REPORTING_CLASSIFICATION
        description: The classification used for pipeline reporting, derived from several opportunity and deal attributes.
        expr: REPORTING_CLASSIFICATION
        data_type: VARCHAR
        sample_values:
          - NL
          - Reno
      - name: SNAPSHOT_YEAR_QUARTER
        description: The year and quarter corresponding to the `snapshot_date` (e.g., 'FY25 Q1').
        expr: SNAPSHOT_YEAR_QUARTER
        data_type: VARCHAR
        sample_values:
          - FY20 Q4
          - FY22 Q2
          - FY25 Q4
      - name: SOLUTION_CATEGORY
        description: The categories of the solutions being sold.
        expr: SOLUTION_CATEGORY
        data_type: VARCHAR
        sample_values:
          - ESG
          - SEC/SEDAR
          - Other
      - name: SOLUTION_GROUP
        description: The standardized logical grouping of the solution (e.g., 'GRC', 'Financial Reporting').
        expr: SOLUTION_GROUP
        data_type: VARCHAR
        sample_values:
          - Sustainability
          - Financial Reporting
    time_dimensions:
      - name: OPPORTUNITY_BOOKINGS_DATE
        description: The date on which the revenue was booked.
        expr: OPPORTUNITY_BOOKINGS_DATE
        data_type: DATE
        sample_values:
          - '2022-07-13'
          - '2026-01-24'
      - name: OPPORTUNITY_CLOSED_DATE
        description: The date the Opportunity was closed.
        expr: OPPORTUNITY_CLOSED_DATE
        data_type: DATE
        sample_values:
          - '2025-05-06'
          - '2022-07-05'
          - '2024-06-21'
      - name: OPPORTUNITY_CREATED_DATE
        description: The date when the Opportunity record was first created.
        expr: OPPORTUNITY_CREATED_DATE
        data_type: DATE
        sample_values:
          - '2025-02-26'
          - '2019-11-05'
          - '2020-10-22'
      - name: OPPORTUNITY_QUALIFIED_DATE
        description: The date the Opportunity first reached a qualified stage (e.g., 'Discovery', 'Alignment').
        expr: OPPORTUNITY_QUALIFIED_DATE
        data_type: DATE
        sample_values:
          - '2023-03-31'
          - '2022-11-07'
      - name: SNAPSHOT_DATE
        description: The calendar date for which the pipeline snapshot was taken.
        expr: SNAPSHOT_DATE
        data_type: DATE
        sample_values:
          - '2021-09-26'
          - '2021-10-07'
          - '2022-06-11'
    facts:
      - name: QUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD
        description: The estimated First Year Subscription & Support (S&S) Net Amount, converted to USD, for opportunities in a qualified stage ('Discovery', 'Alignment', 'Validate', 'Propose', 'Negotiate and Close', 'Define Solution', etc).
        expr: QUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD
        data_type: NUMBER
        sample_values:
          - '319000.00'
          - '57053.33'
      - name: TOTAL_FIRST_YEAR_S_S_NET_AMT_USD
        description: The estimated First Year S&S Net Amount, converted to USD, representing the total pipeline (qualified + unqualified stages).
        expr: TOTAL_FIRST_YEAR_S_S_NET_AMT_USD
        data_type: NUMBER
        sample_values:
          - '132000.00'
          - '43285.33'
          - '62967.21'
      - name: UNQUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD
        description: The estimated First Year S&S Net Amount, converted to USD, for opportunities in an unqualified stage ('Unqualified').
        expr: UNQUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD
        data_type: NUMBER
        sample_values:
          - '72244.84'
          - '57225.69'
    filters: []
    metrics:
      - name: opportunity_count
        description: Count of opportunities in pipeline.
        expr: COUNT(DISTINCT OPPORTUNITY_ID)
      - name: qualified_opportunity_count
        description: Count of qualified opportunities in pipeline.
        expr: COUNT(DISTINCT CASE WHEN IS_QUALIFIED THEN OPPORTUNITY_ID END)
      - name: qualified_pipeline
        description: Pipeline associated with opportunities in a qualified stage ('Discovery', 'Alignment', 'Validate', 'Propose', 'Negotiate and Close', 'Define Solution', etc.)
        expr: SUM(QUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD)
      - name: total_pipeline
        description: Total pipeline. Includes pipeline associated with opportunities in an unqualified and qualified stages.
        expr: SUM(TOTAL_FIRST_YEAR_S_S_NET_AMT_USD)
      - name: unqualified_opportunity_count
        description: Count of unqualified opportunities in pipeline.
        expr: COUNT(DISTINCT CASE WHEN NOT IS_QUALIFIED THEN OPPORTUNITY_ID END)
      - name: unqualified_pipeline
        description: Pipeline associated with opportunities in an unqualified stage ('Unqualified').
        expr: SUM(UNQUALIFIED_FIRST_YEAR_S_S_NET_AMT_USD)
verified_queries:
  - name: What is the open qualified pipeline as of the 5th business day of each quarter, with close date on the same quarter?
    question: What is the open qualified pipeline as of the 5th business day of each quarter, with close date on the same quarter?
    sql: |-
      SELECT
        snapshot_year_quarter,
        snapshot_date,
        SUM(qualified_first_year_s_s_net_amt_usd) AS qualified_pipeline
      FROM
        obt_pipeline
      WHERE
        is_fifth_business_day_of_quarter_snapshot = TRUE
        AND is_open = TRUE
        AND DATE_TRUNC('QUARTER', opportunity_closed_date) = DATE_TRUNC('QUARTER', snapshot_date)
      GROUP BY
        snapshot_year_quarter,
        snapshot_date
      ORDER BY
        snapshot_date DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Isabel Pietri
    verified_at: 1764881763
  - name: What is the unqualified pipeline as of the 5th business day of each quarter?
    question: What is the unqualified pipeline as of the 5th business day of each quarter?
    sql: |-
      SELECT
        snapshot_year_quarter,
        snapshot_date,
        SUM(unqualified_first_year_s_s_net_amt_usd) AS unqualified_pipeline
      FROM
        obt_pipeline
      WHERE
        is_fifth_business_day_of_quarter_snapshot = TRUE
      GROUP BY
        snapshot_year_quarter,
        snapshot_date
      ORDER BY
        snapshot_date DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Isabel Pietri
    verified_at: 1764881920
  - name: What is the open qualified pipeline and number of associated opportunities as of the current day?
    question: What is the open qualified pipeline and number of associated opportunities as of the current day?
    sql: |
      SELECT
        snapshot_date,
        SUM(qualified_first_year_s_s_net_amt_usd) AS qualified_pipeline,
        COUNT(
          DISTINCT CASE
            WHEN IS_QUALIFIED THEN OPPORTUNITY_ID
          END
        ) AS qualified_opportunity_count
      FROM
        obt_pipeline
      WHERE
        is_current_day_snapshot = TRUE
        AND is_open = TRUE
      GROUP BY
        snapshot_date
    use_as_onboarding_question: false
    verified_by: Isabel Pietri
    verified_at: 1764875916
  - name: What is the unqualified pipeline and number of associated opportunities as of the current day?
    question: What is the unqualified pipeline and number of associated opportunities as of the current day?
    sql: |-
      SELECT
        snapshot_date,
        SUM(unqualified_first_year_s_s_net_amt_usd) AS unqualified_pipeline,
        COUNT(
          DISTINCT CASE
            WHEN NOT IS_QUALIFIED THEN OPPORTUNITY_ID
          END
        ) AS unqualified_opportunity_count
      FROM
        obt_pipeline
      WHERE
        is_current_day_snapshot = TRUE
      GROUP BY
        snapshot_date
    use_as_onboarding_question: false
    verified_by: Isabel Pietri
    verified_at: 1764876835
  - name: 'What is the total pipeline and number of associated opportunities created in Q3 of 2025? '
    question: 'What is the total pipeline and number of associated opportunities created in Q3 of 2025? '
    sql: |-
      SELECT
        DATE_TRUNC('QUARTER', opportunity_created_date) AS opportunity_created_quarter,
        SUM(total_first_year_s_s_net_amt_usd) AS total_pipeline,
        COUNT(DISTINCT opportunity_id) AS opportunity_count
      FROM
        obt_pipeline
      WHERE
        DATE_TRUNC('QUARTER', opportunity_created_date) = '2025-07-01'
        AND snapshot_date = opportunity_created_date
      GROUP BY
        opportunity_created_quarter
    use_as_onboarding_question: true
    verified_by: Isabel Pietri
    verified_at: 1764715599
  - name: 'What is the total pipeline and number of associated opportunities created this quarter? '
    question: 'What is the total pipeline and number of associated opportunities created this quarter? '
    sql: |-
      SELECT
        DATE_TRUNC('QUARTER', opportunity_created_date) AS opportunity_created_quarter,
        SUM(total_first_year_s_s_net_amt_usd) AS total_pipeline,
        COUNT(DISTINCT opportunity_id) AS opportunity_count
      FROM
        obt_pipeline
      WHERE
        DATE_TRUNC('QUARTER', opportunity_created_date) = DATE_TRUNC('QUARTER', CURRENT_DATE)
        AND snapshot_date = opportunity_created_date
      GROUP BY
        opportunity_created_quarter
    use_as_onboarding_question: true
    verified_by: Isabel Pietri
    verified_at: 1764715620
  - name: 'What is the pipeline and number of associated opportunities qualified in Q3 of 2025? '
    question: 'What is the pipeline and number of associated opportunities qualified in Q3 of 2025? '
    sql: |-
      SELECT
        DATE_TRUNC('QUARTER', opportunity_qualified_date) AS opportunity_qualified_quarter,
        SUM(qualified_first_year_s_s_net_amt_usd) AS qualified_pipeline,
        COUNT(DISTINCT CASE WHEN IS_QUALIFIED THEN OPPORTUNITY_ID END) AS qualified_opportunity_count
      FROM
        obt_pipeline
      WHERE
        DATE_TRUNC('QUARTER', opportunity_qualified_date) = '2025-07-01'
        AND snapshot_date = opportunity_qualified_date
      GROUP BY
        opportunity_qualified_quarter
    use_as_onboarding_question: true
    verified_by: Isabel Pietri
    verified_at: 1764715782
  - name: 'What is the pipeline and number of associated opportunities qualified this quarter? '
    question: 'What is the pipeline and number of associated opportunities qualified this quarter? '
    sql: |-
      SELECT
        DATE_TRUNC('QUARTER', opportunity_qualified_date) AS opportunity_qualified_quarter,
        SUM(qualified_first_year_s_s_net_amt_usd) AS qualified_pipeline,
        COUNT(
          DISTINCT CASE
            WHEN IS_QUALIFIED THEN OPPORTUNITY_ID
          END
        ) AS qualified_opportunity_count
      FROM
        obt_pipeline
      WHERE
        DATE_TRUNC('QUARTER', opportunity_qualified_date) = DATE_TRUNC('QUARTER', CURRENT_DATE)
        AND snapshot_date = opportunity_qualified_date
      GROUP BY
        opportunity_qualified_quarter
    use_as_onboarding_question: false
    verified_by: Isabel Pietri
    verified_at: 1764878482
{{ generate_verified_query_repository(semantic_view_id=this.identifier) }}
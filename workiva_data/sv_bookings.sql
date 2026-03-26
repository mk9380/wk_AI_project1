
{{ config(materialized='semantic_view') }}

{% set obt_booking_ref = ref("obt_bookings") %}

name: SV_BOOKINGS
description: This semantic view contains information on bookings.
module_custom_instructions:
  question_categorization: |
    - {{ universal_semantic_view_special_instructions() }}
tables:
  - name: OBT_BOOKINGS
    description: The table contains records of customer bookings and sales transactions. Each record includes details about the customer account, including regional, segment, and industry classifications, along with associated sales team members and booking amounts.
    base_table:
      database: {{ obt_booking_ref.database }}
      schema: {{ obt_booking_ref.schema }}
      table: {{ obt_booking_ref.identifier }}
    dimensions:
      - name: ACCOUNT_CORPORATE_COUNTRY
        description: The primary country of the accounts corporation.
        expr: ACCOUNT_CORPORATE_COUNTRY
        data_type: VARCHAR
        sample_values:
          - Belgium
          - United Kingdom
          - Bermuda
      - name: ACCOUNT_NAME
        description: The name of the account.
        expr: ACCOUNT_NAME
        data_type: VARCHAR
        sample_values:
          - Optinose, Inc.
          - Northrop Grumman Corporation
          - camunda services GmbH
      - name: ACCOUNT_REGION
        description: The geographic region of the account.
        expr: ACCOUNT_REGION
        data_type: VARCHAR
        sample_values:
          - LATAM
          - APAC
          - North America
      - name: ACCOUNT_SEGMENT
        description: The business segment of the account.
        expr: ACCOUNT_SEGMENT
        data_type: VARCHAR
        sample_values:
          - Corporate - Expansion
          - Strategic
          - Government
      - name: ACCOUNT_TYPE
        description: The type of account (e.g., Customer, Prospect).
        expr: ACCOUNT_TYPE
        data_type: VARCHAR
        sample_values:
          - Privately owned
          - Publicly Traded Company
          - Private with Public Debt
      - name: ACCOUNT_WORKIVA_INTERNAL_INDUSTRY
        description: The internal Workiva industry classification.
        expr: ACCOUNT_WORKIVA_INTERNAL_INDUSTRY
        data_type: VARCHAR
        sample_values:
          - Investments
          - Banking
      - name: AVERAGE_DEAL_SIZE_FLAG
        description: 'Boolean flag: TRUE if `win_rate = ''Wins''` and `first_year_s_s_net_amt_usd > 0`. Used for deal size calculations.'
        expr: AVERAGE_DEAL_SIZE_FLAG
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: BOOKING_CATEGORY
        description: The booking category derived from the solution classification.
        expr: BOOKING_CATEGORY
        data_type: VARCHAR
        sample_values:
          - SEC/SEDAR
          - Risk
          - Other
      - name: BOOKING_KEY
        description: The unique key from the fct_bookings table, identifying a specific booking record. This ID represents the primary key.
        expr: BOOKING_KEY
        data_type: VARCHAR
        sample_values:
          - c93cf77a56be205b2c07f9ad01005b53
          - 42e47311a24ec94768a45efc20c820ac
          - 9f91d3d0ec618e7023d44e2d3daaddce
      - name: CUSTOMER_ACCOUNT_ID
        description: The ID of the customer account tied to the booking.
        expr: CUSTOMER_ACCOUNT_ID
        data_type: VARCHAR
        sample_values:
          - 0014000000sQ9CMAA0
          - 0014000000Vo4bGAAR
          - 0014000000VpjtrAAB
      - name: CYCLE_TIME
        description: The time (in days) between the Opportunity Qualified Date and the Booking or Closed Date for winning opportunities.
        expr: CYCLE_TIME
        data_type: NUMBER
        sample_values:
          - '245'
          - '165'
          - '39'
      - name: DELIVERY_TYPE
        description: The type of delivery for the solution (e.g., 'DIY/None', 'Partner', 'Workiva').
        expr: DELIVERY_TYPE
        data_type: VARCHAR
        sample_values:
          - DIY/None
          - Workiva Led
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
      - name: HAS_NEW_SOLUTION_CATEGORY
        description: Custom macro-derived flag for new solution category purchase.
        expr: HAS_NEW_SOLUTION_CATEGORY
        data_type: VARCHAR
        sample_values:
          - New Solution Category
          - Expansion
      - name: IS_ADVANCED_SKU_GROUPING
        description: Boolean flag. TRUE if the Opportunity has at least one solution total with an Advanced SKU grouping.
        expr: IS_ADVANCED_SKU_GROUPING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_ALIGNMENT_STAGE_PLUS
        description: Boolean flag indicating if the opportunity is at or past a certain alignment stage.
        expr: IS_ALIGNMENT_STAGE_PLUS
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
      - name: IS_METRIC_BOOKING
        description: 'Indicates if a row is a Booking. Rows with is_metric_booking = True contribute to the Bookings/Total Bookings metric.'
        expr: IS_METRIC_BOOKING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_METRIC_NON_PI_BOOKING
        description: 'Indicates if a row is a Booking. Rows with is_metric_booking = True contribute to the Bookings/Total Bookings metric.'
        expr: IS_METRIC_NON_PI_BOOKING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_METRIC_NEW_LOGO_BOOKING
        description: 'Indicates if a row is a New Logo Booking. Rows with is_metric_new_logo_booking = True contribute to the New Logo Bookings metric.'
        expr: IS_METRIC_NEW_LOGO_BOOKING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_METRIC_NEW_SOLUTION_BOOKING
        description: 'Indicates if a row is a New Solution Booking. Rows with is_metric_new_solution_booking = True contribute to the New Solution Bookings metric.'
        expr: IS_METRIC_NEW_SOLUTION_BOOKING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_METRIC_RENEGOTIATION_BOOKING
        description: 'Indicates if a row is a Renegotiation booking. Rows with is_metric_renegotiation_booking = True contribute to the Renegotiation Bookings metric.'
        expr: IS_METRIC_RENEGOTIATION_BOOKING
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_CLOSED_LOST
        description: A boolean flag indicating if the opportunity is in a closed lost stage.
        expr: IS_CLOSED_LOST
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_CLOSED_PENDING
        description: A boolean flag indicating if the opportunity is in a closed pending stage.
        expr: IS_CLOSED_PENDING
        data_type: BOOLEAN
        sample_values:
          - 'TRUE'
          - 'FALSE'
      - name: IS_CLOSED_WON
        description: A boolean flag indicating if the opportunity is in a closed won stage.
        expr: IS_CLOSED_WON
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_DIY_DELIVERY
        description: 'Boolean flag: TRUE if the delivery_type is ''DIY/None''.'
        expr: IS_DIY_DELIVERY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_ENERGY_INDUSTRY
        description: Boolean flag indicating if the account is in the energy industry.
        expr: IS_ENERGY_INDUSTRY
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: IS_EVENT_REG_AND_GEN_AI
        description: 'Boolean flag: TRUE if ''solution_category'' is ''Event Registration'' or ''Generative AI''.'
        expr: IS_EVENT_REG_AND_GEN_AI
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
          - 'FALSE'
          - 'TRUE'
      - name: IS_ON_THE_PATH
        description: A custom flag (e.g., related to a specific sales/onboarding path).
        expr: IS_ON_THE_PATH
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
          - 'FALSE'
          - 'TRUE'
      - name: OPPORTUNITY_ALIGNMENT_CATEGORY
        description: A further aggregated alignment category derived from the `opportunity_alignment_sub_category`.
        expr: OPPORTUNITY_ALIGNMENT_CATEGORY
        data_type: VARCHAR
        sample_values:
          - Global MSP
          - North America
          - EMEA
      - name: OPPORTUNITY_ALIGNMENT_REGION
        description: The alignment region. This field uses the `opportunity_alignment_category` and `opportunity_market_segment` to determine the region.
        expr: OPPORTUNITY_ALIGNMENT_REGION
        data_type: VARCHAR
        sample_values:
          - LATAM
          - North America
      - name: OPPORTUNITY_ALIGNMENT_SUB_CATEGORY
        description: The alignment sub-category derived from the `opportunity_market_segment`.
        expr: OPPORTUNITY_ALIGNMENT_SUB_CATEGORY
        data_type: VARCHAR
        sample_values:
          - EMEA - North & FSI
          - Corporate Commercial
          - FS
      - name: OPPORTUNITY_DEAL_TYPE
        description: The high-level deal classification (e.g., Consulting, SaaS).
        expr: OPPORTUNITY_DEAL_TYPE
        data_type: VARCHAR
        sample_values:
          - Government
          - Joint Pursuit/Co-Sell
          - Referral Only
      - name: OPPORTUNITY_FORECAST_CATEGORY_NAME
        description: The category used for sales forecasting (e.g., 'Upside', 'Commit').
        expr: OPPORTUNITY_FORECAST_CATEGORY_NAME
        data_type: VARCHAR
        sample_values:
          - Upside
          - Commit
      - name: OPPORTUNITY_ID
        description: The ID of the opportunity.
        expr: OPPORTUNITY_ID
        data_type: VARCHAR
        sample_values:
          - 006PI00000ClZ3RYAV
          - 0066f00001Bs5FAAAZ
          - 0061W00001LiHPoQAN
      - name: OPPORTUNITY_ORIGIN_SOURCE
        description: The origin source of the opportunity.
        expr: OPPORTUNITY_ORIGIN_SOURCE
        data_type: VARCHAR
        sample_values:
          - Organic
          - MCL
      - name: OPPORTUNITY_OWNER_ROLE
        description: The role of the opportunity owner.
        expr: OPPORTUNITY_OWNER_ROLE
        data_type: VARCHAR
        sample_values:
          - Corporate Acquisition East Team
          - ESG Specialist Central & West Team
          - Major Commercial East Team
      - name: OPPORTUNITY_RECORD_TYPE_NAME
        description: The record type of the opportunity.
        expr: OPPORTUNITY_RECORD_TYPE_NAME
        data_type: VARCHAR
        sample_values:
          - Direct
          - New Deal
          - Price Increase
      - name: OPPORTUNITY_OWNER_REGION
        description: The region aligned to the Opportunity. The value is derived from the Opportunity Closer/Owner's region, but falls back to the Account's region for 'Managed Service / Partner Direct' teams or if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_OWNER_REGION
        data_type: VARCHAR
        sample_values:
          - LATAM
          - APAC
          - North America
      - name: OPPORTUNITY_REPORTING_SUB_TYPE
        description: The reporting sub-type of the opportunity.
        expr: OPPORTUNITY_REPORTING_SUB_TYPE
        data_type: VARCHAR
        sample_values:
          - New Deal
          - Renewal
      - name: OPPORTUNITY_MARKET_SEGMENT
        description: The segment aligned to the Opportunity. The value is derived primarily from the Opportunity Closer/Owner's segment, but falls back to the Account Owner's segment if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_MARKET_SEGMENT
        data_type: VARCHAR
        sample_values:
          - EMEA - South
          - Corporate Commercial
          - Canada
      - name: OPPORTUNITY_STAGE_NAME
        description: The stage name of the opportunity.
        expr: OPPORTUNITY_STAGE_NAME
        data_type: VARCHAR
        sample_values:
          - Closed Won
          - Closed Lost/No Decision
          - Closed - Churn
      - name: OPPORTUNITY_OWNER_SUB_REGION
        description: The sub-region aligned to the Opportunity. The value is derived from the Opportunity Closer/Owner's sub-region, but falls back to the Account's region for 'Managed Service / Partner Direct' teams, and to the Accounts sub-region if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_OWNER_SUB_REGION
        data_type: VARCHAR
        sample_values:
          - EMEA-UKI
          - EMEA-FSI North
          - APAC-ANZ
      - name: OPPORTUNITY_TEAM
        description: The team aligned to the Opportunity. The value is derived primarily from the Opportunity Closer/Owner's team, but falls back to the Account Owner's team if `is_account_alignment` is TRUE.
        expr: OPPORTUNITY_TEAM
        data_type: VARCHAR
        sample_values:
          - EMEA - DACH1
          - Strategic Central
          - Corporate South
      - name: OPPORTUNITY_TYPE
        description: The type of opportunity (e.g., New Business, Expansion).
        expr: OPPORTUNITY_TYPE
        data_type: VARCHAR
        sample_values:
          - Renewal
          - Price Increase
          - New Deal
      - name: PERSONA
        description: The target persona for the solution.
        expr: PERSONA
        data_type: VARCHAR
        sample_values:
          - Sustainability
          - Accounting & Finance
          - Other
      - name: PUBLIC_TYPE
        description: The public sector type (e.g., 'SLED', 'Federal') if the `opportunity_team` belongs to a public sector designation, otherwise NULL.
        expr: PUBLIC_TYPE
        data_type: VARCHAR
        sample_values:
          - Federal
          - SLED
      - name: REPORTING_CLASSIFICATION
        synonyms:
          - Reporting Category
        description: The reporting classification used to categorize bookings based on the nature of the booking.
        expr: REPORTING_CLASSIFICATION
        data_type: VARCHAR
        sample_values:
          - PI-Comm Ops
          - NL
          - Reno
          - NS
          - Churn
      - name: SKU_GROUPING
        description: The grouping of SKUs involved in the booking.
        expr: SKU_GROUPING
        data_type: VARCHAR
        sample_values:
          - Annual & Interim Financial Reporting (with ESEF)
          - Workiva Custom Solution
          - SEC Reporting
      - name: SOLUTION_CATEGORY
        description: The broad category of the solution.
        expr: SOLUTION_CATEGORY
        data_type: VARCHAR
        sample_values:
          - Government Management Reporting
          - ESEF
          - Controls Management
      - name: SOLUTION_GROUP
        description: The group the solution belongs to.
        expr: SOLUTION_GROUP
        data_type: VARCHAR
        sample_values:
          - Financial Reporting
          - GRC
          - Sustainability
      - name: SOLUTION_PICKLIST
        description: The value from the solution picklist field.
        expr: SOLUTION_PICKLIST
        data_type: VARCHAR
        sample_values:
          - Government Management Reporting
          - Controls Management
          - ESEF Reporting
      - name: ULTIMATE_PARENT_ACCOUNT_ID_18_DIGIT
        description: The 18-digit ID of the top-level ultimate parent account.
        expr: ULTIMATE_PARENT_ACCOUNT_ID_18_DIGIT
        data_type: VARCHAR
        sample_values:
          - 0014000000kxv71AAA
          - 0014000001gaFdEAAU
          - 0014000000in1i5AAA
      - name: ULTIMATE_PARENT_ACCOUNT_NAME
        description: The name of the ultimate parent account.
        expr: ULTIMATE_PARENT_ACCOUNT_NAME
        data_type: VARCHAR
        sample_values:
          - Cfo Solutions LLC (Partner)
          - Smartsheet Inc.
          - Johnson & Johnson Services, Inc.
      - name: ULTIMATE_PARENT_ACCOUNT_OWNER_ROLE
        description: The account owner role of the ultimate parent.
        expr: ULTIMATE_PARENT_ACCOUNT_OWNER_ROLE
        data_type: VARCHAR
        sample_values:
          - Corporate Expansion West Team
          - Major Commercial East Team
          - Corporate Acquisition East Team
      - name: ULTIMATE_PARENT_ACCOUNT_TYPE
        description: The account type of the ultimate parent.
        expr: ULTIMATE_PARENT_ACCOUNT_TYPE
        data_type: VARCHAR
        sample_values:
          - Publicly Traded Company
          - State Owned Enterprise
          - Private
      - name: ULTIMATE_PARENT_CORPORATE_COUNTRY
        description: The corporate country of the ultimate parent.
        expr: ULTIMATE_PARENT_CORPORATE_COUNTRY
        data_type: VARCHAR
        sample_values:
          - Hong Kong
          - Netherlands
          - United States
      - name: ULTIMATE_PARENT_CSRD
        description: The CSRD classification of the ultimate parent.
        expr: ULTIMATE_PARENT_CSRD
        data_type: VARCHAR
        sample_values:
          - 'Wave 1: Public company/entity in the EU within CSRD scope'
          - 'Wave 2: Private company/entity in the EU within CSRD scope'
      - name: ULTIMATE_PARENT_CUSTOMER_FLAG
        description: Flag indicating if the ultimate parent account is a customer.
        expr: ULTIMATE_PARENT_CUSTOMER_FLAG
        data_type: BOOLEAN
        sample_values:
          - 'FALSE'
          - 'TRUE'
      - name: ULTIMATE_PARENT_INDUSTRY_GROUP
        description: The industry group of the ultimate parent.
        expr: ULTIMATE_PARENT_INDUSTRY_GROUP
        data_type: VARCHAR
        sample_values:
          - Transportation
          - Software and Services
          - Pharmaceuticals, Biotechnology and Life Sciences
      - name: ULTIMATE_PARENT_REGION
        description: The region of the ultimate parent.
        expr: ULTIMATE_PARENT_REGION
        data_type: VARCHAR
        sample_values:
          - APAC
          - EMEA
          - North America
      - name: ULTIMATE_PARENT_SEGMENT
        description: The business segment of the ultimate parent.
        expr: ULTIMATE_PARENT_SEGMENT
        data_type: VARCHAR
        sample_values:
          - Major Accounts FS
          - Corporate - Acquisition
          - Corporate - Expansion
      - name: ULTIMATE_PARENT_WK_COMPANY_TYPE
        description: The Workiva company type of the ultimate parent.
        expr: ULTIMATE_PARENT_WK_COMPANY_TYPE
        data_type: VARCHAR
        sample_values:
          - SLED - City
          - SLED - Authority
      - name: WIN_RATE
        description: Custom macro-derived classification for win rate (e.g., 'Wins', 'Losses') based on opportunity roles and type.
        expr: WIN_RATE
        data_type: VARCHAR
        sample_values:
          - Losses
          - Wins
    time_dimensions:
      - name: OPPORTUNITY_BOOKING_OR_CLOSED_DATE
        description: The primary date used for transaction-time joins (Booking Date if available, else Closed Date).
        expr: OPPORTUNITY_BOOKING_OR_CLOSED_DATE
        data_type: DATE
        sample_values:
          - '2025-07-31'
          - '2025-09-02'
          - '2021-04-01'
      - name: OPPORTUNITY_CREATED_AT
        description: Timestamp when the opportunity was created.
        expr: OPPORTUNITY_CREATED_AT
        data_type: TIMESTAMP_NTZ
        sample_values:
          - 2022-04-01T10:20:08.000+0000
          - 2023-01-07T10:43:25.000+0000
          - 2022-12-05T03:43:00.000+0000
      - name: OPPORTUNITY_QUALIFIED_DATE
        description: Date the opportunity was qualified.
        expr: OPPORTUNITY_QUALIFIED_DATE
        data_type: DATE
        sample_values:
          - '2019-09-17'
          - '2024-04-16'
    facts:
      - name: FIRST_YEAR_S_S_NET_AMT_USD
        description: The First Year Subscription & Support (S&S) Net Amount, converted to USD, for the closed-won booking.
        expr: FIRST_YEAR_S_S_NET_AMT_USD
        data_type: NUMBER
        sample_values:
          - '-117120.00'
          - '2543.60'
          - '64928.00'
    filters:
      - name: IS_METRIC_BOOKING
        description: 'This filters on rows that are bookings. For a row to be a booking, it must have an opportunity stage name of either Closed - Pending or Closed Won and must not be churned. '
        expr: is_metric_booking = TRUE
      - name: NL_bookings
        synonyms:
          - first_time_bookings
          - New logo bookings
          - new_customer_bookings
          - new_logo_bookings
        description: This filters on bookings that are classified as New Logos, or NL. These filtered rows have not churned and have an opportunity stage name of Closed - Pending or Closed Won.
        expr: |-
          is_metric_new_logo_booking
      - name: NS_bookings
        synonyms:
          - new solution
          - New solution bookings
          - ns bookings
        description: The filters on bookings classified as NS, or New Solution. These filtered rows have not churned and have an opportunity stage name of Closed - Pending or Closed Won.
        expr: |-
          is_metric_new_solution_booking
      - name: PI_bookings
        synonyms:
          - PI bookings
          - PI-Comm Ops bookings
        description: This filters on bookings that are classified as PI-Comm Ops, or PI. These filtered rows have not churned and have an opportunity stage name of Closed - Pending or Closed Won.
        expr: |-
          reporting_classification = 'PI-Comm Ops'
          AND is_metric_booking
      - name: Reno_bookings
        synonyms:
          - Renegotiated bookings
          - Reno bookings
        description: This filters bookings that are classified as Renegotiations, or Reno. These filtered rows have not churned and have an opportunity stage name of Closed - Pending or Closed Won.
        expr: |-
          is_metric_renegotiation_booking
    metrics:
      - name: booking_count
        synonyms:
          - bookings volume
          - total volume
          - volume
        description: Count of Bookings
        expr: SUM(CASE WHEN is_metric_booking THEN 1 ELSE 0 END)
      - name: total_bookings
        synonyms:
          - absolute dollars
          - bookings value
          - sum of bookings
        description: The value of the bookings
        expr: SUM(CASE WHEN is_metric_booking THEN first_year_s_s_net_amt_usd ELSE 0 END)
verified_queries:
  - name: How many Non-PI bookings were there in 2025 by reporting classification and month?
    question: How many Non-PI bookings were there in 2025 by reporting classification and month?
    sql: |-
      SELECT
          count(booking_key) AS bookings_volume,
          reporting_classification,
          EXTRACT(MONTH FROM opportunity_booking_or_closed_date) AS month
      FROM {{ ref('obt_bookings') }}
      WHERE
          is_metric_non_pi_booking
          AND EXTRACT(YEAR FROM opportunity_booking_or_closed_date) = 2025
      GROUP BY 2, 3;
    use_as_onboarding_question: true
    verified_by: Maxwell Meiser
    verified_at: 1765494728
  - name: What was the total value of NL bookings in the current quarter?
    question: What was the total value of NL bookings in the current quarter?
    sql: |-
      SELECT
        SUM(first_year_s_s_net_amt_usd) AS total_nl_bookings
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        is_metric_new_logo_booking
        AND DATE_TRUNC('QUARTER', opportunity_booking_or_closed_date) = DATE_TRUNC('QUARTER', CURRENT_DATE)
    use_as_onboarding_question: true
    verified_by: Maxwell Meiser
    verified_at: 1765495077
  - name: What was the volume of Reno bookings in 2024 by quarter?
    question: What was the volume of Reno bookings in 2024 by quarter?
    sql: |-
      SELECT
        DATE_TRUNC('QUARTER', opportunity_booking_or_closed_date) AS quarter,
        COUNT(DISTINCT booking_key) AS reno_bookings_volume
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        is_metric_renegotiation_booking
        AND DATE_PART('YEAR', opportunity_booking_or_closed_date) = 2024
      GROUP BY
        quarter
      ORDER BY
        quarter DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Maxwell Meiser
    verified_at: 1765495515
  - name: What was the total volume of bookings by account region excluding PI bookings in 2025?
    question: What was the total volume of bookings by account region excluding PI bookings in 2025?
    sql: |-
      SELECT
        account_region,
        COUNT(DISTINCT booking_key) AS booking_volume
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        is_metric_non_pi_booking
        AND DATE_PART('YEAR', opportunity_booking_or_closed_date) = 2025
      GROUP BY
        account_region
      ORDER BY
        booking_volume DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Maxwell Meiser
    verified_at: 1765495972
  - name: Count and show the value of NS bookings in Q3 2025.
    question: Count and show the value of NS bookings in Q3 2025.
    sql: |-
      SELECT
        COUNT(DISTINCT booking_key) AS ns_bookings_count,
        SUM(first_year_s_s_net_amt_usd) AS ns_bookings_value
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        is_metric_new_solution_booking
        AND DATE_TRUNC('QUARTER', opportunity_booking_or_closed_date) = '2025-07-01'
    use_as_onboarding_question: false
    verified_by: Maxwell Meiser
    verified_at: 1765496037
  - name: What is the value of Reno and NS bookings in 2024 by account region?
    question: What is the value of Reno and NS bookings in 2024 by account region?
    sql: |-
      SELECT
        account_region,
        SUM(first_year_s_s_net_amt_usd) AS total_bookings_value
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        (is_metric_new_solution_booking OR is_metric_renegotiation_booking)
        AND DATE_PART('YEAR', opportunity_booking_or_closed_date) = 2024
      GROUP BY
        account_region
      ORDER BY
        total_bookings_value DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Maxwell Meiser
    verified_at: 1765564256
  - name: What the the accounts with the highest NS bookings in the current quarter?
    question: What are the accounts with the highest NS bookings in the current quarter?
    sql: |-
      SELECT
        account_name,
        SUM(first_year_s_s_net_amt_usd) AS ns_bookings_value
      FROM
        {{ ref('obt_bookings') }}
      WHERE
        is_metric_new_solution_booking
        AND DATE_TRUNC('QUARTER', opportunity_booking_or_closed_date) = DATE_TRUNC('QUARTER', CURRENT_DATE)
      GROUP BY
        account_name
      ORDER BY
        ns_bookings_value DESC NULLS LAST
    use_as_onboarding_question: false
    verified_by: Maxwell Meiser
    verified_at: 1765564437
{{ generate_verified_query_repository(semantic_view_id=this.identifier) }}

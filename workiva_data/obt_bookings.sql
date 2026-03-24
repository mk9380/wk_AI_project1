{{
  config(
    materialized='table'
  )
}}

-- Get bookings from fct table
WITH fct_bookings AS (
    SELECT *
    FROM {{ ref('fct_bookings') }}
),

-- Get opportunity information
opportunity_history AS (
    SELECT *
    FROM {{ ref('dim_opportunities') }}
    WHERE is_latest = true
),

-- Get solution classification information
solution_classifications_history AS (
    SELECT *
    FROM {{ ref('dim_opportunity_solution_classifications') }}
    WHERE is_latest = true
),

-- Get account information
account_history AS (
    SELECT *
    FROM {{ ref('dim_accounts') }}
    WHERE is_latest = true
),

-- Get opportunity alignment information
opportunity_alignments_history AS (
    SELECT *
    FROM {{ ref('dim_opportunity_alignments') }}
    WHERE is_latest = true
),

-- Get current Value Management assist status
opportunity_assist_latest AS (
    SELECT *
    FROM {{ ref('dim_opportunity_assist_summary') }}
    WHERE is_latest = TRUE
),

-- Get partners information
opportunity_partners AS (
    SELECT *
    FROM {{ ref('dim_opportunity_partners') }}
    WHERE is_latest = true

),

-- Get opportunity delivery type
line_item_summary AS (
    SELECT *
    FROM {{ ref('dim_opportunity_line_item_summary') }}
    WHERE is_latest = true
),

-- Get contact information 
contact_history AS (
    SELECT *
    FROM {{ ref('dim_contacts') }}
    WHERE is_latest = true
),

-- Get contract information
contract_history AS (
    SELECT
        *,
        COALESCE(end_client_id, account_id) AS end_client_or_account_id
    FROM {{ ref('dim_contracts') }}
    WHERE is_latest = true
    AND contract_workiva_status NOT IN ('Never Started', 'None')

) ,

solution_category_contract_lines AS (
    SELECT
        subscription_line.contract_id,
        subscription_line.solution_category,
        subscription_line.start_date AS contract_line_solution_category_subscription_start_date
    FROM {{ ref('salesforce_sbqq_subscription') }} AS subscription_line

),

solutions_for_win_rate AS (
    SELECT
        contract_history.end_client_or_account_id,
        solution_category_dates.solution_category,
        MIN(solution_category_dates.contract_line_solution_category_subscription_start_date) AS sfwr_start_date
    FROM contract_history
    JOIN solution_category_contract_lines AS solution_category_dates
        ON contract_history.contract_id = solution_category_dates.contract_id
    WHERE solution_category IS NOT NULL
    GROUP BY contract_history.end_client_or_account_id, solution_category_dates.solution_category
),

obt_bookings AS (
    SELECT
        -- Unique Key
        fct_bookings.booking_key,

        -- Bookings amounts and currency iso code
        fct_bookings.currency_iso_code,
        fct_bookings.first_year_s_s_net_amt_usd,

        -- Account Information
        fct_bookings.customer_account_id,
        account_history.account_name,
        account_history.account_region,
        account_history.account_segment,
        account_history.account_corporate_country,
        account_history.account_corporate_country_region,
        account_history.account_sales_territory,
        account_history.account_type,
        account_history.account_workiva_internal_industry,
        account_history.account_inside_sales_owner_name,
        account_history.account_sec_owner_name,
        account_history.account_capital_markets_owner_name,
        account_history.account_integrated_risk_owner_name,
        account_history.account_renewal_owner_name,
        account_history.account_owner_name,
        account_history.account_partner_owner_name,
        account_history.account_global_stat_owner_name,
        account_history.account_financial_services_owner_name,
        account_history.account_esg_mgmt_reporting_owner_name,
        account_history.is_financial_industry,
        account_history.is_energy_industry,
        account_history.financial_industry_type,
        account_history.energy_industry_type,
        account_history.is_on_the_path,
        account_history.account_annual_revenue,
        account_history.account_annual_revenue_usd,
        account_history.account_annual_revenue_band,
        account_history.parent_account_id,
        account_history.ultimate_parent_account_id,
        account_history.ultimate_parent_account_id_18_digit,
        account_history.ultimate_parent_account_name,
        account_history.ultimate_parent_annual_revenue,
        account_history.ultimate_parent_account_type,
        account_history.ultimate_parent_corporate_country,
        account_history.ultimate_parent_region,
        account_history.ultimate_parent_csrd,
        account_history.ultimate_parent_segment,
        account_history.ultimate_parent_account_owner_role,
        account_history.ultimate_parent_wk_company_type,
        account_history.ultimate_parent_customer_flag,
        account_history.ultimate_parent_industry_group,

        -- Booking Flags / Attributes
        fct_bookings.is_closed_won,
        fct_bookings.is_closed_pending,
        fct_bookings.is_closed_lost,
        {{ is_event_reg_and_gen_ai('solution_classifications_history.solution_category') }},
        fct_bookings.delivery_partner,
        fct_bookings.delivery_type,
        fct_bookings.is_diy_delivery,
        fct_bookings.large_deal_category,
        {{ has_new_solution_category('sfwr.end_client_or_account_id','opportunity_history.opportunity_closed_date','sfwr.sfwr_start_date') }} AS has_new_solution_category,
        {{ 
        win_rate_classification(
            'opportunity_history.opportunity_owner_role',
            'opportunity_history.opportunity_closing_role',
            'opportunity_history.opportunity_type',
            'has_new_solution_category',
            'opportunity_history.opportunity_stage_name',
            'opportunity_history.opportunity_qualified_date'
        ) 
        }} AS win_rate,
        COALESCE(
            win_rate = 'Wins'
            AND COALESCE(fct_bookings.first_year_s_s_net_amt_usd, 0) > 0,
            FALSE
        ) AS average_deal_size_flag,
              CASE
            WHEN
                win_rate = 'Wins'
                AND COALESCE(fct_bookings.first_year_s_s_net_amt_usd, 0) > 0
                THEN DATEDIFF('day', opportunity_history.opportunity_qualified_date, fct_bookings.booking_or_closed_date)
        END AS cycle_time,

        -- Solution Classifications
        fct_bookings.solution_total_id,
        fct_bookings.opportunity_solution_classification_key,
        solution_classifications_history.solution_total_name,
        solution_classifications_history.solution_category,
        solution_classifications_history.booking_category,
        solution_classifications_history.budgeted_booking_category,
        solution_classifications_history.solution_group,
        solution_classifications_history.sku_grouping,
        solution_classifications_history.solution_picklist,
        solution_classifications_history.reporting_classification_bookings AS reporting_classification,
        solution_classifications_history.persona,
        solution_classifications_history.is_multi_category,
        solution_classifications_history.is_multi_solution_category,
        solution_classifications_history.is_multi_booking_category,
        solution_classifications_history.is_multi_sku_grouping,
        solution_classifications_history.is_multi_solution,
        solution_classifications_history.is_advanced_sku_grouping,
        solution_classifications_history.is_standard_sku_grouping,
        solution_classifications_history.opportunity_has_advanced_sku_grouping,
        solution_classifications_history.opportunity_has_standard_sku_grouping,

        -- Opportunity Date/Time related fields
        opportunity_history.opportunity_created_at,
        opportunity_history.opportunity_created_date,
        opportunity_history.opportunity_created_quarter_date,
        opportunity_history.opportunity_created_year_quarter,
        fct_bookings.closed_date AS opportunity_closed_date,
        opportunity_history.opportunity_closed_quarter_date,
        opportunity_history.opportunity_closed_year_quarter,
        opportunity_history.opportunity_qualified_date,
        opportunity_history.opportunity_qualified_quarter_date,
        opportunity_history.opportunity_qualified_year_quarter,
        fct_bookings.bookings_date AS opportunity_bookings_date,
        fct_bookings.booking_or_closed_date AS opportunity_booking_or_closed_date,
        DATE_TRUNC('QUARTER', opportunity_booking_or_closed_date) AS opportunity_booking_or_closed_quarter_date,
        {{ get_fiscal_year_quarter('opportunity_booking_or_closed_date')}} AS opportunity_booking_or_closed_year_quarter,

        -- Opportunity Information
        fct_bookings.opportunity_id,
        opportunity_history.opportunity_stage_name,
        opportunity_history.opportunity_name,
        opportunity_history.opportunity_record_type_name,
        opportunity_history.opportunity_type,
        opportunity_history.opportunity_competitor,
        opportunity_history.opportunity_owner_role,
        opportunity_history.opportunity_closing_role,
        opportunity_history.opportunity_lead_source,
        opportunity_history.opportunity_deal_type,
        opportunity_history.opportunity_forecast_category_name,
        opportunity_history.opportunity_origin_source,
        opportunity_history.is_alignment_stage_plus,
        opportunity_history.opportunity_service_delivery_notes,
        opportunity_history.opportunity_reporting_sub_type,
        opportunity_history.is_deal_registration,
        opportunity_history.is_managed_service,

        -- Opportunity alignments
        opportunity_alignments_history.opportunity_market_segment,
        opportunity_alignments_history.opportunity_team,
        opportunity_alignments_history.opportunity_owner_region,
        opportunity_alignments_history.opportunity_owner_sub_region,
        opportunity_alignments_history.public_type,
        opportunity_alignments_history.is_public_type,
        opportunity_alignments_history.opportunity_alignment_sub_category,
        opportunity_alignments_history.opportunity_alignment_category,
        opportunity_alignments_history.opportunity_alignment_region,

        -- Opportunity Assist
        COALESCE(opportunity_assist_latest.has_completed_value_management_assist, FALSE) AS has_completed_value_management_assist,

        -- First associated contact information
        fct_bookings.first_associated_contact_id,
        contact_history.contact_lead_source AS first_associated_contact_lead_source,
        contact_history.contact_lead_source_condensed AS first_associated_contact_lead_source_condensed,

        -- Partners information
        {% for i in range(1, var('n_partners') + 1) %}
            partner_{{ i }}_account_id,
            partner_{{ i }}_account_name,
            partner_{{ i }}_relationship,
            partner_{{ i }}_type,
            partner_{{ i }}_account_region,
            partner_{{ i }}_ultimate_parent_account_id,
            partner_{{ i }}_geographic_tag,
        {% endfor %}
        opportunity_partners.partner_delivering_services,
        opportunity_partners.partnership_notes,
        opportunity_partners.opportunity_partner_relationship,
        COALESCE(opportunity_partners.partner_1_account_id IS NOT NULL, FALSE) AS is_partner_related,

        -- Line item aggregations at opp level
        line_item_summary.opportunity_partner_delivery_type,
        line_item_summary.opportunity_products,
        line_item_summary.opportunity_subscription_products,
        line_item_summary.opportunity_capital_markets_products
    FROM fct_bookings 
    LEFT JOIN opportunity_history
        ON 
            fct_bookings.opportunity_id = opportunity_history.opportunity_id
    LEFT JOIN opportunity_alignments_history
        ON
            fct_bookings.opportunity_id = opportunity_alignments_history.opportunity_id
    LEFT JOIN opportunity_assist_latest
        ON 
            fct_bookings.opportunity_id = opportunity_assist_latest.opportunity_id   
    LEFT JOIN solution_classifications_history
        ON 
            fct_bookings.opportunity_solution_classification_key = solution_classifications_history.opportunity_solution_classification_key       
    LEFT JOIN contact_history
        ON 
            fct_bookings.first_associated_contact_id = contact_history.contact_id
    LEFT JOIN solutions_for_win_rate AS sfwr
        ON
            opportunity_history.customer_account_id = sfwr.end_client_or_account_id
            AND solution_classifications_history.solution_category = sfwr.solution_category   
    INNER JOIN account_history
        ON
            fct_bookings.customer_account_id = account_history.account_id  
    LEFT JOIN opportunity_partners
        ON
            fct_bookings.opportunity_id = opportunity_partners.opportunity_id
    LEFT JOIN line_item_summary
        ON
            fct_bookings.opportunity_id = line_item_summary.opportunity_id
    WHERE 
        solution_classifications_history.reporting_classification_bookings IS NOT NULL
        AND is_event_reg_and_gen_ai = FALSE
               
)

SELECT
    *,
    -- Metric Flags
    {{ is_metric_booking('opportunity_stage_name', 'reporting_classification') }} AS is_metric_booking,
    {{ is_metric_non_pi_booking('is_metric_booking', 'reporting_classification', 'opportunity_alignment_category') }} AS is_metric_non_pi_booking,
    {{ is_metric_new_logo_booking('is_metric_booking', 'reporting_classification') }} AS is_metric_new_logo_booking,
    {{ is_metric_new_solution_booking('is_metric_booking', 'reporting_classification') }} AS is_metric_new_solution_booking,
    {{ is_metric_renegotiation_booking('is_metric_booking', 'reporting_classification') }} AS is_metric_renegotiation_booking,
    {{ is_metric_partner_delivery('opportunity_stage_name', 'reporting_classification') }} AS is_metric_partner_delivery
FROM obt_bookings

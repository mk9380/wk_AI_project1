{{
  config(
    materialized='table'
  )
}}

-- Get pipeline from fct table
WITH fct_pipeline AS (
    SELECT *
    FROM {{ ref('fct_pipeline') }}
),

-- Get opportunity information
opportunity_history AS (
    SELECT *
    FROM {{ ref('dim_opportunities') }}
),

-- Get opportunity alignment information
opportunity_alignments_history AS (
    SELECT *
    FROM {{ ref('dim_opportunity_alignments') }}
),

-- Get Value Management Assist information
opportunity_assist_hist AS (
    SELECT *
    FROM {{ ref('dim_opportunity_assist_summary') }}
),

-- Get contact information 
contact_history AS (
    SELECT *
    FROM {{ ref('dim_contacts') }}
),

-- Get user information
user_history AS (
    SELECT *
    FROM {{ ref('dim_users') }}
),

-- Get classifications
solution_classifications_history AS (
    SELECT *
    FROM {{ ref('dim_opportunity_solution_classifications') }}
),

-- Get account information
account_history AS (
    SELECT *
    FROM {{ ref('dim_accounts') }}
),

-- Get partners information
opportunity_partners AS (
    SELECT *
    FROM {{ ref('dim_opportunity_partners') }}
),

line_item_summary AS (
    SELECT *
    FROM {{ ref('dim_opportunity_line_item_summary')}}
),

obt_pipeline AS (
    SELECT 
        -- Unique Key
        fct_pipeline.pipeline_key,

        -- Snapshot related fields and flags
        fct_pipeline.snapshot_date,
        fct_pipeline.snapshot_year_quarter,
        fct_pipeline.is_current_day_snapshot,
        fct_pipeline.is_first_day_of_month_snapshot,
        fct_pipeline.is_first_day_of_quarter_snapshot,
        fct_pipeline.is_fifth_business_day_of_month_snapshot,
        fct_pipeline.is_fifth_business_day_of_quarter_snapshot,
        fct_pipeline.is_one_year_ago_snapshot,
        fct_pipeline.is_two_years_ago_snapshot,
        fct_pipeline.is_three_years_ago_snapshot,
        fct_pipeline.is_quarterly_snapshot,
        fct_pipeline.is_monthly_snapshot,
        fct_pipeline.is_closed_opp_snapshot_at_closed_date,
        fct_pipeline.is_closed_opp_snapshot_latest,

        -- Pipeline amounts and currency iso code
        fct_pipeline.currency_iso_code,
        fct_pipeline.qualified_first_year_s_s_net_amt_usd,
        fct_pipeline.unqualified_first_year_s_s_net_amt_usd,
        fct_pipeline.total_first_year_s_s_net_amt_usd,

        -- Pipeline Flags
        fct_pipeline.is_opportunity_with_solution_total,
        fct_pipeline.is_open,
        fct_pipeline.is_closed_won,
        fct_pipeline.is_closed_pending,
        fct_pipeline.is_closed_lost,
        fct_pipeline.is_qualified,
        fct_pipeline.is_snapshot_near_closed_date,
        fct_pipeline.is_positive_opportunity_s_s_net,
        fct_pipeline.is_quarterly_close_target_snapshot,
        fct_pipeline.is_created_on_snapshot,
        CASE 
            WHEN fct_pipeline.snapshot_date = opportunity_history.opportunity_qualified_date 
            THEN TRUE 
            ELSE FALSE 
        END AS is_qualified_on_snapshot,
        fct_pipeline.is_metric_created_pipeline,
        fct_pipeline.is_metric_open_qualified_pipeline,
        fct_pipeline.is_metric_open_total_pipeline,
        fct_pipeline.is_metric_start_of_quarter_open_qualified_pipeline,
        fct_pipeline.is_metric_start_of_quarter_open_total_pipeline,

        -- Opportunity Information
        fct_pipeline.opportunity_id,
        fct_pipeline.opportunity_stage_name,
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
        fct_pipeline.large_deal_category,

        -- Opportunity Date/Time related fields
        fct_pipeline.opportunity_closed_date,
        opportunity_history.opportunity_created_at,
        opportunity_history.opportunity_created_date,
        opportunity_history.opportunity_created_quarter_date,
        opportunity_history.opportunity_created_year_quarter,
        opportunity_history.opportunity_closed_quarter_date,
        opportunity_history.opportunity_closed_year_quarter,
        opportunity_history.opportunity_qualified_date,
        opportunity_history.opportunity_qualified_quarter_date,
        opportunity_history.opportunity_qualified_year_quarter,
        opportunity_history.opportunity_bookings_date,
        opportunity_history.opportunity_booking_or_closed_date,

        -- Sales team related information
        fct_pipeline.opportunity_created_by_user_id,
        opp_creator_user_hist.user_name AS opportunity_creator_name,
        opp_creator_user_hist.user_username AS opportunity_creator_username,
        fct_pipeline.opportunity_owner_user_id,
        opp_owner_user_hist.user_name AS opportunity_owner_name,
        opp_owner_user_hist.user_username AS opportunity_owner_username,
        opp_owner_manager_user_hist.user_name AS opportunity_owner_manager_name,
        opp_owner_manager_user_hist.user_username AS opportunity_owner_manager_username,
        {{ opportunity_creator_source(
        account_owner_id_col='account_history.account_owner_user_id', 
        opportunity_creator_id_col='fct_pipeline.opportunity_created_by_user_id', 
        opportunity_creator_role_col='opp_creator_user_hist.user_role'
        ) }} AS opportunity_creator_source,

        -- Opportunity alignments
        opportunity_alignments_history.opportunity_market_segment,
        opportunity_alignments_history.opportunity_team,
        opportunity_alignments_history.opportunity_owner_region,
        opportunity_alignments_history.opportunity_owner_sub_region,
        opportunity_alignments_history.public_type,
        opportunity_alignments_history.is_public_type,
        opportunity_alignments_history.opportunity_alignment_region,
        opportunity_alignments_history.opportunity_alignment_category,
        opportunity_alignments_history.opportunity_alignment_sub_category,

        -- Opportunity Assist
        COALESCE(opportunity_assist_hist.has_completed_value_management_assist, FALSE) AS has_completed_value_management_assist,

        -- Solution Classifications
        fct_pipeline.solution_total_id,
        fct_pipeline.opportunity_solution_classification_key,
        solution_classifications_history.solution_total_name,
        solution_classifications_history.solution_category,
        solution_classifications_history.booking_category,
        solution_classifications_history.budgeted_booking_category,
        solution_classifications_history.solution_group,
        solution_classifications_history.sku_grouping,
        solution_classifications_history.solution_picklist,
        solution_classifications_history.reporting_classification_pipeline AS reporting_classification,
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

        -- First associated contact information
        fct_pipeline.first_associated_contact_id,
        contact_history.contact_lead_source AS first_associated_contact_lead_source,
        contact_history.contact_lead_source_condensed AS first_associated_contact_lead_source_condensed,
        
        -- Account Information
        fct_pipeline.customer_account_id,
        account_history.account_name,
        account_history.account_region,
        account_history.account_segment,
        account_history.account_corporate_country,
        account_history.account_corporate_country_region,
        account_history.account_sales_territory,
        account_history.account_type,
        account_history.account_workiva_internal_industry,
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
        COALESCE(opportunity_partners.partner_1_account_id IS NOT NULL, FALSE) AS is_partner_related,

        -- Line item aggregations at opp level
        line_item_summary.opportunity_partner_delivery_type,
        line_item_summary.opportunity_products,
        line_item_summary.opportunity_subscription_products,
        line_item_summary.opportunity_capital_markets_products

    FROM fct_pipeline
    LEFT JOIN opportunity_history
        ON 
            fct_pipeline.opportunity_id = opportunity_history.opportunity_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opportunity_history.version_start_at
            AND opportunity_history.version_end_at
    LEFT JOIN user_history AS opp_creator_user_hist
        ON
            fct_pipeline.opportunity_created_by_user_id = opp_creator_user_hist.user_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opp_creator_user_hist.version_start_at
            AND opp_creator_user_hist.version_end_at
    LEFT JOIN user_history AS opp_owner_user_hist
        ON
            fct_pipeline.opportunity_owner_user_id = opp_owner_user_hist.user_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opp_owner_user_hist.version_start_at
            AND opp_owner_user_hist.version_end_at
    LEFT JOIN user_history AS opp_owner_manager_user_hist
        ON
            opp_owner_user_hist.manager_user_id = opp_owner_manager_user_hist.user_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opp_owner_manager_user_hist.version_start_at
            AND opp_owner_manager_user_hist.version_end_at
    LEFT JOIN opportunity_alignments_history
        ON
            fct_pipeline.opportunity_id = opportunity_alignments_history.opportunity_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opportunity_alignments_history.version_start_at
            AND opportunity_alignments_history.version_end_at
    LEFT JOIN opportunity_assist_hist
        ON fct_pipeline.opportunity_id = opportunity_assist_hist.opportunity_id
        AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opportunity_assist_hist.version_start_at
        AND opportunity_assist_hist.version_end_at
    LEFT JOIN solution_classifications_history
        ON fct_pipeline.opportunity_solution_classification_key = solution_classifications_history.opportunity_solution_classification_key
        AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN solution_classifications_history.version_start_at
            AND solution_classifications_history.version_end_at
    LEFT JOIN contact_history
        ON fct_pipeline.first_associated_contact_id = contact_history.contact_id
        AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN contact_history.version_start_at
            AND contact_history.version_end_at
    LEFT JOIN account_history
        ON
            fct_pipeline.customer_account_id = account_history.account_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN account_history.version_start_at
            AND account_history.version_end_at
    LEFT JOIN opportunity_partners
        ON
            fct_pipeline.opportunity_id = opportunity_partners.opportunity_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN opportunity_partners.version_start_at
            AND opportunity_partners.version_end_at 
    LEFT JOIN line_item_summary
        ON
            fct_pipeline.opportunity_id = line_item_summary.opportunity_id
            AND fct_pipeline.snapshot_end_of_day_timestamp BETWEEN line_item_summary.version_start_at
            AND line_item_summary.version_end_at 
)

SELECT *
FROM obt_pipeline

{{ config(
    tags=["six_hourly"]
) }}


WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE

), distinct_values AS (

  SELECT DISTINCT
    -- Discovery and Qualification
    cp_why_do_anything_at_all,
    cp_why_now,
    cp_identify_pain,
    cp_metrics,
    cp_value_driver,
    cp_why_gitlab,

    -- Stakeholder Analysis
    cp_champion,
    cp_economic_buyer,
    cp_partner,

    -- Decision Making Process
    cp_decision_process,
    cp_decision_criteria,
    cp_paper_process,

    -- Use Cases & Assistance
    cp_use_cases,
    cp_help,

    -- Deal Execution
    cp_close_plan,
    cp_review_notes,

    -- Risk and Overall Assessment  
    cp_risks,
    cp_score

  FROM prep_crm_opportunity

)
SELECT    
  {{ dbt_utils.generate_surrogate_key(get_opportunity_command_plan_fields()) }} AS dim_crm_command_plan_sk,
  * 
FROM distinct_values
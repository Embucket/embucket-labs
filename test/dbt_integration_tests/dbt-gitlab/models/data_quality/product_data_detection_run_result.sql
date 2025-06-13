{{ config(
    tags=["mnpi_exception"]
) }}

WITH detection_rule AS (

    SELECT *
    FROM {{ ref('data_detection_rule') }}

), rule_run_detail AS (

   SELECT
        rule_id,
        processed_record_count,
        passed_record_count,
        failed_record_count,
        ((passed_record_count/processed_record_count)*100) AS percent_of_records_passed,
        ((failed_record_count/processed_record_count)*100) AS percent_of_records_failed,
        rule_run_date,
        type_of_data
   FROM {{ ref('product_data_detection_run_detail') }}

), final AS (

    SELECT DISTINCT
        detection_rule.rule_id,
        detection_rule.rule_name,
        detection_rule.rule_description,
        rule_run_detail.rule_run_date,
        rule_run_detail.percent_of_records_passed,
        rule_run_detail.percent_of_records_failed,
        IFF(percent_of_records_passed > threshold, TRUE, FALSE) AS is_pass,
        rule_run_detail.type_of_data
    FROM rule_run_detail
    LEFT OUTER JOIN  detection_rule ON
    rule_run_detail.rule_id = detection_rule.rule_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@snalamaru",
    created_date="2021-06-16",
    updated_date="2021-06-16"
) }}

{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','account_scorecard_history') }}
),

renamed AS (

  SELECT
    id::NUMBER                                                  AS id,
    _fivetran_deleted::BOOLEAN                                  AS _fivetran_deleted,
    user_deployments_utilization::NUMBER                        AS user_deployments_utilization,
    customer_experience_gc::NUMBER                              AS customer_experience_gc,
    persona_engagement_gc::NUMBER                               AS persona_engagement_gc,
    cd_adoption_detail::NUMBER                                  AS cd_adoption_detail,
    deployments_per_user_last_28_days::NUMBER                   AS deployments_per_user_last_28_days,
    modified_date::TIMESTAMP                                    AS modified_date,
    deployments_utilization_gc::NUMBER                          AS deployments_utilization_gc,
    ci_adoption_details::NUMBER                                 AS ci_adoption_details,
    ci_adoption::NUMBER                                         AS ci_adoption,
    support_sev_1_gc::NUMBER                                    AS support_sev_1_gc,
    license_usage_gc::NUMBER                                    AS license_usage_gc,
    user_engagement_gc::NUMBER                                  AS user_engagement_gc,
    secure_scanners_utilization_user_l_28_d_gc::NUMBER          AS secure_scanners_utilization_user_l_28_d_gc,
    tam_sentiment::NUMBER                                       AS tam_sentiment,
    user_engagementaa_08_c_4::NUMBER                            AS user_engagementaa_08_c_4,
    deployments_per_user_last_28_days_gc::NUMBER                AS deployments_per_user_last_28_days_gc,
    created_by_id_gc::VARCHAR                                   AS created_by_id_gc,
    engagement_gc::NUMBER                                       AS engagement_gc,
    snapshot_date_gc::DATE                                      AS snapshot_date_gc,
    ci_pipeline_usage_user_l_28_d::NUMBER                       AS ci_pipeline_usage_user_l_28_d,
    scm_adoption::NUMBER                                        AS scm_adoption,
    cd_adoption::NUMBER                                         AS cd_adoption,
    secret_detection_jobs_utilization_user_l_28_d_gc::NUMBER    AS secret_detection_jobs_utilization_user_l_28_d_gc,
    container_scanning_jobs_utilization_user_l_28_d_gc::NUMBER  AS container_scanning_jobs_utilization_user_l_28_d_gc,
    successful_deployments::NUMBER                              AS successful_deployments,
    overall_score_account_gc::NUMBER                            AS overall_score_account_gc,
    security_adoption::NUMBER                                   AS security_adoption,
    customer_outcomes_gc::NUMBER                                AS customer_outcomes_gc,
    scorecard_id_gc::VARCHAR                                    AS scorecard_id_gc,
    roi::NUMBER                                                 AS roi,
    support_issues_gc::NUMBER                                   AS support_issues_gc,
    dev_sec_ops_adoption_details_gc::NUMBER                     AS dev_sec_ops_adoption_details_gc,
    product_usage_gc::NUMBER                                    AS product_usage_gc,
    product_sentiment_gc::NUMBER                                AS product_sentiment_gc,
    time_granularity_gc::VARCHAR                                AS time_granularity_gc,
    support_experience_gc::NUMBER                               AS support_experience_gc,
    account_id_gc::VARCHAR                                      AS account_id_gc,
    risk_gc::NUMBER                                             AS risk_gc,
    created_date::TIMESTAMP                                     AS created_date,
    modified_by_id_gc::VARCHAR                                  AS modified_by_id_gc,
    _fivetran_synced::TIMESTAMP                                 AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed

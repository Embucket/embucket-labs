{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','success_plan') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                                     AS gsid,
    _fivetran_deleted::BOOLEAN                        AS _fivetran_deleted,
    over_due_type::VARCHAR                            AS over_due_type,
    q_1_q_2_approved_gc::DATE                         AS q_1_q_2_approved_gc,
    closed_won_objective_count::NUMBER                AS closed_won_objective_count,
    q_2_q_3_approved_gc::VARCHAR                      AS q_2_q_3_approved_gc,
    name::VARCHAR                                     AS name,
    start_date::TIMESTAMP                             AS start_date,
    initiatives_gc::VARCHAR                           AS initiatives_gc,
    obstacles_gc::VARCHAR                             AS obstacles_gc,
    created_by_id::VARCHAR                            AS created_by_id,
    closed_lost_objective_count::NUMBER               AS closed_lost_objective_count,
    it_or_digital_transformation_strategy_gc::VARCHAR AS it_or_digital_transformation_strategy_gc,
    success_plan_type_id::VARCHAR                     AS success_plan_type_id,
    high_level_initiatives_gc::VARCHAR                AS high_level_initiatives_gc,
    modified_by_id::VARCHAR                           AS modified_by_id,
    is_overdue::BOOLEAN                               AS is_overdue,
    action_plan::VARCHAR                              AS action_plan,
    days_allocated::NUMBER                            AS days_allocated,
    entity_type::VARCHAR                              AS entity_type,
    sharing_type::VARCHAR                             AS sharing_type,
    industry_trends_gc::VARCHAR                       AS industry_trends_gc,
    created_date::TIMESTAMP                           AS created_date,
    status_id::VARCHAR                                AS status_id,
    account_summary_history_gc::VARCHAR               AS account_summary_history_gc,
    highlights::VARCHAR                               AS highlights,
    description::VARCHAR                              AS description,
    days_past_due_date::NUMBER                        AS days_past_due_date,
    business_overview_gc::VARCHAR                     AS business_overview_gc,
    is_closed::BOOLEAN                                AS is_closed,
    modified_date::TIMESTAMP                          AS modified_date,
    source::VARCHAR                                   AS source,
    open_objective_count::NUMBER                      AS open_objective_count,
    is_closed_overdue::BOOLEAN                        AS is_closed_overdue,
    goals_strategies_objectives_gc::VARCHAR           AS goals_strategies_objectives_gc,
    description_string_gc::VARCHAR                    AS description_string_gc,
    owner_id::VARCHAR                                 AS owner_id,
    due_date::TIMESTAMP                               AS due_date,
    open_task_count::NUMBER                           AS open_task_count,
    total_objective_count::NUMBER                     AS total_objective_count,
    overall_account_plan_goals_gc::VARCHAR            AS overall_account_plan_goals_gc,
    company_id::VARCHAR                               AS company_id,
    due_date_variance::NUMBER                         AS due_date_variance,
    close_source::VARCHAR                             AS close_source,
    original_due_date::TIMESTAMP                      AS original_due_date,
    active_date::TIMESTAMP                            AS active_date,
    closed_task_count::NUMBER                         AS closed_task_count,
    days_due::NUMBER                                  AS days_due,
    competitive_landscape_gc::VARCHAR                 AS competitive_landscape_gc,
    strategic_partnerships_gc::VARCHAR                AS strategic_partnerships_gc,
    percent_complete::FLOAT                           AS percent_complete,
    current_entitled_solutions_gc::VARCHAR            AS current_entitled_solutions_gc,
    highlights_string_gc::VARCHAR                     AS highlights_string_gc,
    closed_date::TIMESTAMP                            AS closed_date,
    _fivetran_synced::TIMESTAMP                       AS _fivetran_synced,
    success_plan_link_gc                              AS success_plan_link_gc
  FROM source
)

SELECT *
FROM renamed

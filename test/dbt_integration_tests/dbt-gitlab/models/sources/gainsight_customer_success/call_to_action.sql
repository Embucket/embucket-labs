{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','call_to_action') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                         AS gsid,
    _fivetran_deleted::BOOLEAN            AS _fivetran_deleted,
    over_due_type::VARCHAR                AS over_due_type,
    name::VARCHAR                         AS name,
    start_date::TIMESTAMP                 AS start_date,
    is_recurring::BOOLEAN                 AS is_recurring,
    created_by_id::VARCHAR                AS created_by_id,
    next_task_due_date::DATE              AS next_task_due_date,
    reason_id::VARCHAR                    AS reason_id,
    is_draft::BOOLEAN                     AS is_draft,
    total_task_count::NUMBER              AS total_task_count,
    type_id::VARCHAR                      AS type_id,
    modified_by_id::VARCHAR               AS modified_by_id,
    is_overdue::BOOLEAN                   AS is_overdue,
    stage_name_gc::VARCHAR                AS stage_name_gc,
    rule_action_id::VARCHAR               AS rule_action_id,
    cta_group_id::VARCHAR                 AS cta_group_id,
    template_display_order::VARCHAR       AS template_display_order,
    objective_category_id::VARCHAR        AS objective_category_id,
    days_allocated::NUMBER                AS days_allocated,
    sharing_type::VARCHAR                 AS sharing_type,
    entity_type::VARCHAR                  AS entity_type,
    created_date::TIMESTAMP               AS created_date,
    playbook_id::VARCHAR                  AS playbook_id,
    status_id::VARCHAR                    AS status_id,
    target_value_gc::VARCHAR              AS target_value_gc,
    competitor_gc::VARCHAR                AS competitor_gc,
    is_important::BOOLEAN                 AS is_important,
    is_snoozed::BOOLEAN                   AS is_snoozed,
    days_past_due_date::NUMBER            AS days_past_due_date,
    summary_of_outcome_gc::VARCHAR        AS summary_of_outcome_gc,
    is_closed::BOOLEAN                    AS is_closed,
    modified_date::TIMESTAMP              AS modified_date,
    source::VARCHAR                       AS source,
    is_closed_overdue::BOOLEAN            AS is_closed_overdue,
    owner_id::VARCHAR                     AS owner_id,
    due_date::TIMESTAMP                   AS due_date,
    priority_id::VARCHAR                  AS priority_id,
    open_task_count::NUMBER               AS open_task_count,
    age::NUMBER                           AS age,
    risk_type_gc::VARCHAR                 AS risk_type_gc,
    company_id::VARCHAR                   AS company_id,
    comments::VARCHAR                     AS comments,
    due_date_variance::NUMBER             AS due_date_variance,
    close_source::VARCHAR                 AS close_source,
    original_due_date::TIMESTAMP          AS original_due_date,
    success_criteria::VARCHAR             AS success_criteria,
    closed_task_count::NUMBER             AS closed_task_count,
    intended_implementation_date_gc::DATE AS intended_implementation_date_gc,
    days_due::NUMBER                      AS days_due,
    current_value_gc::VARCHAR             AS current_value_gc,
    percent_complete::NUMBER              AS percent_complete,
    closed_date::TIMESTAMP                AS closed_date,
    _fivetran_synced::TIMESTAMP           AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed

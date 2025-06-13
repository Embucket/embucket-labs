{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','cs_task') }}
),

renamed AS (

  SELECT
    gsid::VARCHAR                 AS gsid,
    _fivetran_deleted::BOOLEAN    AS _fivetran_deleted,
    is_important::BOOLEAN         AS is_important,
    description::VARCHAR          AS description,
    over_due_type::VARCHAR        AS over_due_type,
    days_past_due_date::VARCHAR   AS days_past_due_date,
    timeline_activity_id::VARCHAR AS timeline_activity_id,
    dependency_meta::VARCHAR      AS dependency_meta,
    is_closed::BOOLEAN            AS is_closed,
    modified_date::TIMESTAMP      AS modified_date,
    cta_id::VARCHAR               AS cta_id,
    source::VARCHAR               AS source,
    name::VARCHAR                 AS name,
    start_date::TIMESTAMP         AS start_date,
    is_closed_overdue::BOOLEAN    AS is_closed_overdue,
    customer_dri_gc::VARCHAR      AS customer_dri_gc,
    owner_id::VARCHAR             AS owner_id,
    created_by_id::VARCHAR        AS created_by_id,
    milestone_risk_gc::VARCHAR    AS milestone_risk_gc,
    playbook_task_id::VARCHAR     AS playbook_task_id,
    due_date::TIMESTAMP           AS due_date,
    priority_id::VARCHAR          AS priority_id,
    parent_id::VARCHAR            AS parent_id,
    type_id::VARCHAR              AS type_id,
    modified_by_id::VARCHAR       AS modified_by_id,
    is_overdue::BOOLEAN           AS is_overdue,
    display_order::NUMBER         AS display_order,
    is_email_sent::BOOLEAN        AS is_email_sent,
    company_id::VARCHAR           AS company_id,
    due_date_variance::NUMBER     AS due_date_variance,
    close_source::VARCHAR         AS close_source,
    sync_status::NUMBER           AS sync_status,
    original_due_date::TIMESTAMP  AS original_due_date,
    progress_2_gc::VARCHAR        AS progress_2_gc,
    days_allocated::NUMBER        AS days_allocated,
    entity_type::VARCHAR          AS entity_type,
    days_due::VARCHAR             AS days_due,
    gainsight_task_id::VARCHAR    AS gainsight_task_id,
    last_update_source::NUMBER    AS last_update_source,
    created_date::TIMESTAMP       AS created_date,
    playbook_id::VARCHAR          AS playbook_id,
    status_id::VARCHAR            AS status_id,
    dyna_metadata::VARCHAR        AS dyna_metadata,
    closed_date::TIMESTAMP        AS closed_date,
    _fivetran_synced::TIMESTAMP   AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed

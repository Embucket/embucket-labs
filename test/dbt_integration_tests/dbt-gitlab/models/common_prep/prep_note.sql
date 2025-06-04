{{ config(
    tags=["product"]
) }}

{{ config({
    "materialized": "incremental",
    "unique_key": "dim_note_id",
    "on_schema_change": "sync_all_columns"
    })
}}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('dim_epic', 'dim_epic'),
    ('dim_namespace', 'dim_namespace'),
    ('gitlab_dotcom_system_note_metadata_source', 'gitlab_dotcom_system_note_metadata_source')
]) }}

, gitlab_dotcom_notes_dedupe_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_notes_dedupe_source') }}
    {% if is_incremental() %}

    WHERE updated_at > (SELECT MAX(updated_at) FROM {{this}})

    {% endif %}

), joined AS (

    SELECT
      gitlab_dotcom_notes_dedupe_source.id::NUMBER                                            AS dim_note_id,
      gitlab_dotcom_notes_dedupe_source.author_id::NUMBER                                     AS author_id,
      gitlab_dotcom_notes_dedupe_source.project_id::NUMBER                                    AS dim_project_id,
      IFNULL(prep_project.ultimate_parent_namespace_id::NUMBER,
              dim_namespace.ultimate_parent_namespace_id::NUMBER)                             AS ultimate_parent_namespace_id,
      gitlab_dotcom_notes_dedupe_source.noteable_id::NUMBER                                   AS noteable_id,
      dim_date.date_id::NUMBER                                                                AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)::NUMBER                                 AS dim_plan_id,
      IFF(noteable_type = '', NULL, noteable_type)::VARCHAR                                   AS noteable_type,
      gitlab_dotcom_notes_dedupe_source.created_at::TIMESTAMP                                 AS created_at,
      gitlab_dotcom_notes_dedupe_source.updated_at::TIMESTAMP                                 AS updated_at,
      gitlab_dotcom_notes_dedupe_source.note::VARCHAR                                         AS note,
      gitlab_dotcom_notes_dedupe_source.attachment::VARCHAR                                   AS attachment,
      gitlab_dotcom_notes_dedupe_source.line_code::VARCHAR                                    AS line_code,
      gitlab_dotcom_notes_dedupe_source.commit_id::VARCHAR                                    AS commit_id,
      true::BOOLEAN                                       AS is_system_note,
      gitlab_dotcom_notes_dedupe_source.updated_by_id::NUMBER                                 AS note_updated_by_id,
      gitlab_dotcom_notes_dedupe_source.position::VARCHAR                                     AS position_number,
      gitlab_dotcom_notes_dedupe_source.original_position::VARCHAR                            AS original_position,
      gitlab_dotcom_notes_dedupe_source.resolved_at::TIMESTAMP                                AS resolved_at,
      gitlab_dotcom_notes_dedupe_source.resolved_by_id::NUMBER                                AS resolved_by_id,
      gitlab_dotcom_notes_dedupe_source.discussion_id::VARCHAR                                AS discussion_id,
      gitlab_dotcom_notes_dedupe_source.cached_markdown_version::NUMBER                       AS cached_markdown_version,
      true::BOOLEAN                             AS resolved_by_push,
      gitlab_dotcom_system_note_metadata_source.action_type::VARCHAR                          AS action_type,
      true::BOOLEAN                               AS is_deleted,
      gitlab_dotcom_notes_dedupe_source.pgp_is_deleted_updated_at::TIMESTAMP                  AS is_deleted_updated_at
    FROM gitlab_dotcom_notes_dedupe_source
    LEFT JOIN prep_project ON gitlab_dotcom_notes_dedupe_source.project_id = prep_project.dim_project_id
    LEFT JOIN dim_epic ON gitlab_dotcom_notes_dedupe_source.noteable_id = dim_epic.epic_id
    LEFT JOIN dim_namespace
        ON dim_epic.dim_namespace_sk = dim_namespace.dim_namespace_sk
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_notes_dedupe_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_notes_dedupe_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(gitlab_dotcom_notes_dedupe_source.created_at) = dim_date.date_day
    LEFT JOIN gitlab_dotcom_system_note_metadata_source
      ON gitlab_dotcom_notes_dedupe_source.id = gitlab_dotcom_system_note_metadata_source.note_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mpeychet_",
    updated_by="@utkarsh060",
    created_date="2021-06-22",
    updated_date="2024-07-09"
) }}

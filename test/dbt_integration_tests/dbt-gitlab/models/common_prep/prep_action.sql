{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('plans', 'gitlab_dotcom_plans_source'),
    ('prep_project', 'prep_project'),
    ('gitlab_dotcom_events_source', 'gitlab_dotcom_events_dedupe_source'),
    ('dim_date', 'dim_date'),
    ('prep_user', 'prep_user')
]) }}

, joined AS (

    SELECT
      gitlab_dotcom_events_source.id                                                              AS dim_action_id,

      -- FOREIGN KEYS
      gitlab_dotcom_events_source.project_id::NUMBER                                              AS dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      prep_user.dim_user_id                                                                       AS dim_user_id,
      dim_date.date_id                                                                            AS created_date_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                                             AS dim_plan_id,

      -- events metadata
      gitlab_dotcom_events_source.target_id::NUMBER                                               AS target_id,
      gitlab_dotcom_events_source.target_type::VARCHAR                                            AS target_type,
      gitlab_dotcom_events_source.created_at::TIMESTAMP                                           AS created_at,
      {{action_type(action_type_id='action')}}::VARCHAR                                           AS event_action_type
    FROM gitlab_dotcom_events_source
    LEFT JOIN prep_project ON gitlab_dotcom_events_source.project_id = prep_project.dim_project_id
    LEFT JOIN prep_user
      ON gitlab_dotcom_events_source.author_id = prep_user.dim_user_id
    LEFT JOIN dim_namespace_plan_hist ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
        AND gitlab_dotcom_events_source.created_at >= dim_namespace_plan_hist.valid_from
        AND gitlab_dotcom_events_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    INNER JOIN dim_date ON TO_DATE(gitlab_dotcom_events_source.created_at) = dim_date.date_day

)

SELECT * FROM joined

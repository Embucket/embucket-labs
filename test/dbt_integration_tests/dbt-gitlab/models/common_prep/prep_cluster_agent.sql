{{ config(
    tags=["product"]
) }}

{{ simple_cte([
  ('cluster_agents','gitlab_dotcom_cluster_agents_source'),
  ('prep_project', 'prep_project'),
  ('dim_date', 'dim_date'),
  ('dim_namespace_plan_hist', 'dim_namespace_plan_hist')
  ])
}}

, renamed AS (

    SELECT
      cluster_agents.cluster_agent_id,
      prep_project.dim_project_id,
      prep_project.dim_namespace_id,
      prep_project.ultimate_parent_namespace_id,
      cluster_agents.created_by_user_id                           AS dim_user_id,
      IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)             AS dim_plan_id,
      dim_date.date_id                                            AS created_date_id,
      cluster_agents.created_at,
      cluster_agents.updated_at
    FROM cluster_agents
    LEFT JOIN prep_project
      ON cluster_agents.project_id = prep_project.dim_project_id
    LEFT JOIN dim_namespace_plan_hist
      ON prep_project.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND cluster_agents.created_at >= dim_namespace_plan_hist.valid_from
      AND cluster_agents.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
    LEFT JOIN dim_date
      ON cluster_agents.created_at::DATE = dim_date.date_day

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2022-11-16",
    updated_date="2022-11-16"
) }}

{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('gitlab_dotcom_members_source', 'gitlab_dotcom_members_source'),
    ('dim_namespace_plan_hist', 'dim_namespace_plan_hist'),
    ('dim_namespace', 'dim_namespace'),
    ('dim_date', 'dim_date')
]) }}

, joined AS (

  SELECT
    gitlab_dotcom_members_source.member_id                          AS dim_member_id,

    -- FOREIGN KEYS
    dim_namespace.dim_namespace_id                                  AS dim_namespace_id,
    dim_namespace.ultimate_parent_namespace_id,
    gitlab_dotcom_members_source.user_id                            AS dim_user_id,
    dim_date.date_id                                                AS created_date_id,
    IFNULL(dim_namespace_plan_hist.dim_plan_id, 34)                 AS dim_plan_id,

    -- events metadata
    gitlab_dotcom_members_source.invite_accepted_at::TIMESTAMP      AS created_at
  FROM gitlab_dotcom_members_source
  LEFT JOIN dim_namespace ON gitlab_dotcom_members_source.source_id = dim_namespace.dim_namespace_id
  LEFT JOIN dim_namespace_plan_hist ON dim_namespace.ultimate_parent_namespace_id = dim_namespace_plan_hist.dim_namespace_id
      AND gitlab_dotcom_members_source.created_at >= dim_namespace_plan_hist.valid_from
      AND gitlab_dotcom_members_source.created_at < COALESCE(dim_namespace_plan_hist.valid_to, '2099-01-01')
  INNER JOIN dim_date ON TO_DATE(gitlab_dotcom_members_source.invite_accepted_at) = dim_date.date_day
  WHERE gitlab_dotcom_members_source.is_currently_valid = TRUE
    AND gitlab_dotcom_members_source.member_source_type = 'Namespace'
    AND gitlab_dotcom_members_source.invite_accepted_at IS NOT NULL

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@utkarsh060",
    updated_by="@utkarsh060",
    created_date="2024-02-28",
    updated_date="2024-02-28"
) }}

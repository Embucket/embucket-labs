{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('mart','wk_mart_namespace_company_account')
])}}

SELECT *
FROM mart 
WHERE namespace_is_ultimate_parent = TRUE
  AND namespace_is_internal = FALSE
  AND visibility_level IN ('public', 'private')
  AND gitlab_plan_title != 'Default'
  AND namespace_creator_is_blocked = FALSE
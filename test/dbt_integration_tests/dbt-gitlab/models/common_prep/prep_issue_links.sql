{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('gitlab_dotcom_issue_links_source', 'gitlab_dotcom_issue_links_source'),
    ('prep_issue', 'prep_issue'),

]) }}

, renamed AS (

    SELECT
      gitlab_dotcom_issue_links_source.issue_link_id        AS dim_issue_link_id,
      -- FOREIGN KEYS
      gitlab_dotcom_issue_links_source.source_id            AS issue_id_source,
      {{ get_keyed_nulls('source_issue.dim_issue_sk') }}    AS dim_issue_sk_source,
      gitlab_dotcom_issue_links_source.target_id            AS issue_id_target,
      {{ get_keyed_nulls('target_issue.dim_issue_sk') }}    AS dim_issue_sk_target,
      gitlab_dotcom_issue_links_source.created_at,
      gitlab_dotcom_issue_links_source.updated_at,
      valid_from
    FROM gitlab_dotcom_issue_links_source
    LEFT JOIN prep_issue source_issue
      ON gitlab_dotcom_issue_links_source.source_id = source_issue.issue_id
    LEFT JOIN prep_issue target_issue
      ON gitlab_dotcom_issue_links_source.target_id = target_issue.issue_id


)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@michellecooper",
    created_date="2021-08-04",
    updated_date="2023-10-13"
) }}

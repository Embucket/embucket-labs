{{ config(
    tags=["product"]
) }}

WITH gitlab_dotcom_label_links_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source')}}

), renamed AS (

    SELECT
      gitlab_dotcom_label_links_source.label_link_id     AS dim_label_link_id,
      -- FOREIGN KEYS
      gitlab_dotcom_label_links_source.label_id          AS dim_label_id,
      -- foreign key to different table depending on target type of label
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'Issue'
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS issue_id,
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'MergeRequest'
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS merge_request_id,
      CASE
        WHEN gitlab_dotcom_label_links_source.target_type = 'Epic'
        THEN gitlab_dotcom_label_links_source.target_id
        ELSE NULL
      END AS epic_id,
      --
      gitlab_dotcom_label_links_source.target_type,
      gitlab_dotcom_label_links_source.label_link_created_at       AS label_added_at,
      gitlab_dotcom_label_links_source.label_link_updated_at       AS label_updated_at
      --

    FROM gitlab_dotcom_label_links_source
    -- exclude broken links (deleted labels)
    WHERE gitlab_dotcom_label_links_source.label_id IS NOT NULL
    -- only include currently active labels to avoid duplicate label_link_ids
      AND is_currently_valid = TRUE


)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@michellecooper",
    created_date="2021-08-04",
    updated_date="2023-10-30"
) }}

WITH labels AS (

    SELECT *
    FROM {{ ref('prep_labels') }} 

), label_links AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source') }} 

), base_labels AS (

    SELECT
      label_links.target_id AS issue_id,
      labels.label_title,
      labels.label_type,
      label_links.label_link_created_at AS label_added_at,
      label_links.valid_to,
      label_links.is_currently_valid
    FROM labels
    LEFT JOIN label_links
      ON labels.dim_label_id = label_links.label_id
      AND label_links.target_type = 'Issue'
    JOIN {{ ref('internal_issues_enhanced') }} AS issue ON label_links.target_id = issue.issue_id AND issue.ultimate_parent_id = 52999839 -- SIRT namespace
    WHERE label_links.target_id IS NOT NULL

)
SELECT *
FROM base_labels
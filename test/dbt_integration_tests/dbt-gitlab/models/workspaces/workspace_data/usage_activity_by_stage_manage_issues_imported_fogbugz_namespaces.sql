SELECT
    namespace_id,
    TO_DATE(CURRENT_DATE) AS run_day,
    COUNT(
        DISTINCT gitlab_dotcom_projects_dedupe_source.creator_id
    ) AS counter_value
FROM
    {{ref('gitlab_dotcom_projects_dedupe_source')}}
LEFT JOIN
    {{ref('gitlab_dotcom_namespaces_dedupe_source')}} ON
        gitlab_dotcom_namespaces_dedupe_source.id = gitlab_dotcom_projects_dedupe_source.namespace_id
WHERE
    gitlab_dotcom_projects_dedupe_source.import_type = 'fogbugz' AND gitlab_dotcom_projects_dedupe_source.import_type IS NOT NULL
GROUP BY 1

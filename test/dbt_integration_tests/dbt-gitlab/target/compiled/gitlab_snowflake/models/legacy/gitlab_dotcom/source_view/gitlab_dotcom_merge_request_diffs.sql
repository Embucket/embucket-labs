WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_merge_request_diffs_source

)

SELECT *
FROM source
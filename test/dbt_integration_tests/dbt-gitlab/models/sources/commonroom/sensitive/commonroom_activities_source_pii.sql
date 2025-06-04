{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ ref('commonroom_activities_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_timestamp, first_activity_date, full_name ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_activities_source', 'full_name') }}
    FROM source
)

SELECT DISTINCT *
  FROM source_pii
{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ ref('commonroom_community_members_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY profiles, primary_email, full_name, location ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_community_members_source', 'full_name') }}
    FROM source

)

SELECT DISTINCT *
  FROM source_pii
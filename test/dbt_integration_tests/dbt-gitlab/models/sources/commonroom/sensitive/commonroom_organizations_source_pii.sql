{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

    SELECT *
    FROM {{ source('commonroom', 'organizations') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY domain, organization_name ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), source_pii AS (

    SELECT {{ nohash_sensitive_columns('commonroom_organizations_source', 'organization_name') }}
    FROM source
)

SELECT DISTINCT *
  FROM source_pii
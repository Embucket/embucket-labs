WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_audit_source') }}

)

SELECT
  source.audit_id,
  audut_manager.value['id']::NUMBER AS audit_manager_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.audit_managers)) audut_manager
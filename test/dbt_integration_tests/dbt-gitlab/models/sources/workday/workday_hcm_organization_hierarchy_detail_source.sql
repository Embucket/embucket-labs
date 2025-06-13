WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','organization_hierarchy_detail') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    organization_id::VARCHAR        AS organization_id,
    type::VARCHAR                   AS type,
    linked_organization_id::VARCHAR AS linked_organization_id,
    _fivetran_deleted::BOOLEAN      AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP     AS _fivetran_synced
  FROM source

)

SELECT *
FROM final

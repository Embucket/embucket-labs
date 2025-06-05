WITH source AS (

    SELECT *
    FROM EMBUCKET.tap_postgres.customers_db_license_versions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id,
                                            created_at,
                                            item_type,
                                            event,
                                            whodunnit,
                                            object,
                                            object_changes
                               ORDER BY _uploaded_at DESC) = 1
), dedupe AS (

    SELECT *
      FROM source

), renamed AS (

    SELECT
      md5(cast(coalesce(cast(item_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(item_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(created_at as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(item_type as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(event as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(whodunnit as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(object as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(object_changes as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
                                                          AS id,
      item_id::NUMBER                                     AS item_id,
      created_at::TIMESTAMP                               AS created_at,
      item_type::VARCHAR                                  AS item_type,
      event::VARCHAR                                      AS event,
      whodunnit::VARCHAR                                  AS whodunnit,
      CASE
      WHEN whodunnit ILIKE '%@gitlab.com' THEN
        TRIM(REPLACE(whodunnit,'Admin: ',''))
      ELSE
        NULL
      END                                                 AS whodunnit_gitlab,
      CASE
      WHEN whodunnit ILIKE '%@gitlab.com' THEN
        whodunnit
      ELSE
        NULL
      END                                                 AS whodunnit_gitlab_desc,
      object::VARCHAR                                     AS object,
      object_changes::VARCHAR                             AS object_changes,
      DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS _uploaded_at
    FROM dedupe

)

SELECT *
FROM renamed
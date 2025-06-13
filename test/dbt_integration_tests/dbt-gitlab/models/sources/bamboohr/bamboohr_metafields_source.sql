WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'meta_fields') }}
    ORDER BY uploaded_at DESC
    LIMIT 1
  
), renamed AS (

    SELECT
      metafield.value['id']::INT               AS metafield_id,
      metafield.value['name']::VARCHAR         AS metafield_name,
      metafield.value['alias']::VARCHAR        AS metafield_alias_name
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) metafield

)

SELECT *
FROM renamed

WITH source AS (

  SELECT 
   cloud_connector_unit_primitives.*,
   f.value                            AS unit_primitives
  FROM {{ source('gitlab_data_yaml', 'cloud_connector_unit_primitives') }},
  LATERAL FLATTEN(input => jsontext) AS f
  WHERE f.key = 'unit_primitives'

),

renamed AS (

  SELECT 
    f1.value['name']::VARCHAR                               AS unit_primitive_name,
    f1.value['description']::VARCHAR                        AS unit_primitive_description,
    f1.value['feature_category']::VARCHAR                   AS feature_category,
    f1.value['group']::VARCHAR                              AS engineering_group,
    f1.value['backend_services']::VARCHAR                   AS backend_services,
    f1.value['license_types']::VARCHAR                      AS available_to_license_types,
    f1.value['add_ons']::VARCHAR                            AS available_to_add_ons,
    f1.value['cut_off_date']::DATE                          AS cut_off_date,
    f1.value['documentation_url']::VARCHAR                  AS documentation_url,
    f1.value['introduced_by_url']::VARCHAR                  AS introduced_by_url,
    f1.value['unit_primitive_issue_url']::VARCHAR           AS unit_primitive_issue_url,
    f1.value['deprecated_by_url']::VARCHAR                  AS deprecated_by_url,
    f1.value['deprecation_message']::VARCHAR                AS deprecation_message,
    f1.value['min_gitlab_version']::FLOAT                   AS min_gitlab_version,
    f1.value['min_gitlab_version_for_free_access']::FLOAT   AS min_gitlab_version_for_free_access
  FROM source,
  LATERAL FLATTEN(input => unit_primitives) AS f1
  QUALIFY ROW_NUMBER() OVER (PARTITION BY unit_primitive_name ORDER BY source.uploaded_at DESC) = 1

)

SELECT
  renamed.*
FROM renamed

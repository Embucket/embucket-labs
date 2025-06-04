SELECT DISTINCT
      flattened_keystone.value:name::VARCHAR        AS content_name,
      flattened_keystone.value:gitlab_epic::VARCHAR AS gitlab_epic,
      flattened_keystone.value:gtm::VARCHAR         AS gtm,
      flattened_keystone.value:language::VARCHAR    AS language,
      flattened_keystone.value:type::VARCHAR        AS type,
      flattened_keystone.value:url_slug::VARCHAR    AS url_slug,
      flattened_keystone.value                      AS full_value
FROM {{ source('gitlab_data_yaml', 'content_keystone') }},
    LATERAL FLATTEN(input => PARSE_JSON(content_keystone.jsontext)) AS flattened_keystone
WHERE LENGTH(url_slug) > 0
QUALIFY MAX(uploaded_at) OVER() = uploaded_at

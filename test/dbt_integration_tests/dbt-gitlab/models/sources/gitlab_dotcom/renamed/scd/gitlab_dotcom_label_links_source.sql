WITH
{{ distinct_source(source=source('gitlab_dotcom', 'label_links'))}}

, renamed AS (

    SELECT

      id::NUMBER                                    AS label_link_id,
      label_id::NUMBER                              AS label_id,
      target_id::NUMBER                             AS target_id,
      target_type::VARCHAR                          AS target_type,
      created_at::TIMESTAMP                         AS label_link_created_at,
      updated_at::TIMESTAMP                         AS label_link_updated_at,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='label_link_id',
    primary_key_raw='id'
) }}

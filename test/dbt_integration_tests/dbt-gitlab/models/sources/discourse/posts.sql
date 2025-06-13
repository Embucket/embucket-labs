WITH source AS (

    SELECT *
    FROM {{ source('discourse', 'posts') }}

), parsed AS (

    SELECT
      json_value.value['start_date']::DATETIME  AS report_start_date,
      json_value.value['title']::VARCHAR        AS report_title,
      json_value.value['type']::VARCHAR         AS report_type,
      json_value.value['total']::VARCHAR        AS report_total,
      data_level_one.value['x']::DATE           AS report_value_date,
      data_level_one.value['y']::INT            AS report_value,
      uploaded_at                               AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), OUTER => TRUE) json_value,
    LATERAL FLATTEN(json_value.value:data,'') data_level_one

), dedupe AS (

    SELECT DISTINCT
      report_start_date,
      report_title,
      report_type,
      report_value_date,
      report_value,
      MAX(uploaded_at)      AS last_uploaded_at
    FROM parsed
    {{ dbt_utils.group_by(n=5) }}
)

SELECT *
FROM dedupe

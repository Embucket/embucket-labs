WITH source AS (

    SELECT *
    FROM {{ source('discourse', 'consolidated_page_views') }}

), parsed AS (

    SELECT
      json_value.value['start_date']::DATETIME  AS report_start_date,
      json_value.value['title']::VARCHAR        AS report_title,
      json_value.value['type']::VARCHAR         AS report_type,
      data_level_one.value['req']::VARCHAR      AS request_type,
      data_level_one.value['label']::VARCHAR    AS request_label,
      data_level_two.value['x']::DATE           AS report_value_date,
      data_level_two.value['y']::INT            AS report_value,
      uploaded_at                               AS uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), OUTER => TRUE) json_value,
    LATERAL FLATTEN(json_value.value:data,'') data_level_one,
    LATERAL FLATTEN(data_level_one.value:data, '') data_level_two

), dedupe AS (

    SELECT DISTINCT
      report_start_date,
      report_title,
      report_type,
      request_type,
      request_label,
      report_value_date,
      report_value,
      MAX(uploaded_at)      AS last_uploaded_at
    FROM parsed
    {{ dbt_utils.group_by(n=7) }}

)

SELECT * 
FROM dedupe

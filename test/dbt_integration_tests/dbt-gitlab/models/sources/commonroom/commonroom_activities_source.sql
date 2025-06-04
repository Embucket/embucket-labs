{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(

  SELECT *
  FROM {{ source('commonroom', 'activities') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY activity_timestamp, first_activity_date, full_name ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), dedupe AS
(

    SELECT {{ dbt_utils.generate_surrogate_key(['activity_timestamp', 'first_activity_date','full_name']) }} AS primary_key,
           activity_timestamp::TIMESTAMP_NTZ                                                                 AS activity_timestamp,
           activity_type::VARCHAR                                                                            AS activity_type,
           first_activity_date::TIMESTAMP_NTZ                                                                AS first_activity_date,
           full_name::VARCHAR                                                                                AS full_name,
           member_id::VARCHAR                                                                                AS member_id,
           profiles::VARCHAR                                                                                 AS profiles,
           service_name::VARCHAR                                                                             AS service_name,
           _uploaded_at::TIMESTAMP                                                                           AS _uploaded_at,
           _file_name::VARCHAR                                                                               AS _file_name
    FROM source

)

SELECT *
  FROM dedupe
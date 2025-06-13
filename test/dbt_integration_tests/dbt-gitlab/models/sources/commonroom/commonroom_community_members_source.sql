{{ config(
    tags=["commonroom"]
) }}

WITH source AS
(
  SELECT *
  FROM {{ source('commonroom', 'community_members') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY profiles, primary_email, full_name, location ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), dedupe AS
(

    SELECT {{ dbt_utils.generate_surrogate_key(['profiles', 'primary_email','full_name', 'location']) }} AS primary_key,
           first_activity_date::TIMESTAMP_NTZ                                                AS first_activity_date,
           first_activity_source::VARCHAR                                                    AS first_activity_source,
           full_name::VARCHAR                                                                AS full_name,
           impact_points_all_time::NUMBER                                                    AS impact_points_all_time,
           last_activity_date::TIMESTAMP_NTZ                                                 AS last_activity_date,
           location::VARCHAR                                                                 AS location,
           member_ids::VARCHAR                                                               AS member_ids,
           primary_email::VARCHAR                                                            AS primary_email,
           profiles::VARCHAR                                                                 AS profiles,
           segment_names::VARCHAR                                                            AS segment_names,
           tags::VARCHAR                                                                     AS tags,
           job_title::VARCHAR                                                                AS job_title,
           last_organization::VARCHAR                                                        AS last_organization,
           organization_name::VARCHAR                                                        AS organization_name,
           organization_domain::VARCHAR                                                      AS organization_domain,
           _uploaded_at::TIMESTAMP                                                           AS _uploaded_at,
           _file_name::VARCHAR                                                               AS _file_name
    FROM source

)

SELECT *
  FROM dedupe
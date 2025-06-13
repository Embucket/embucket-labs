{{ config(
    tags=["commonroom"]
) }}


WITH source AS
(
  SELECT *
  FROM {{ source('commonroom', 'organizations') }}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY domain, organization_name ORDER BY _uploaded_at DESC, _file_name DESC) = 1

), dedupe AS
(

    SELECT {{ dbt_utils.generate_surrogate_key(['domain', 'organization_name']) }} AS primary_key,
           approx_revenue_max::NUMBER                                              AS approx_revenue_max,
           approx_revenue_min::NUMBER                                              AS approx_revenue_min,
           domain::VARCHAR                                                         AS domain,
           employee_count::NUMBER                                                  AS employee_count,
           first_seen_date::TIMESTAMP_NTZ                                          AS first_seen_date,
           first_seen_source::VARCHAR                                              AS first_seen_source,
           last_seen_date::TIMESTAMP_NTZ                                           AS last_seen_date,
           location::VARCHAR                                                       AS location,
           member_count::NUMBER                                                    AS member_count,
           organization_name::VARCHAR                                              AS organization_name,
           _uploaded_at::TIMESTAMP                                                 AS _uploaded_at,
           _file_name::VARCHAR                                                     AS _file_name
    FROM source
)

SELECT *
  FROM dedupe
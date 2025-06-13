WITH source AS (

  SELECT *
  FROM {{ source('demo_architecture_portal', 'labs') }}

), renamed AS (

  SELECT
    id::NUMBER                        AS id,
    date::VARCHAR                     AS lab_date,
    attendance::VARCHAR               AS attendance,
    region::VARCHAR                   AS region,
    topic::VARCHAR                    AS topic,
    sfdc_id::VARCHAR                  AS sfdc_id,
    delivery::VARCHAR                 AS delivery,
    opp::VARCHAR                      AS opp,
    covered_topics::VARCHAR           AS covered_topics,
    host_users::VARCHAR               AS host_users, 
    managers::VARCHAR                 AS managers,
    url::VARCHAR                      AS url

  FROM source

)


SELECT *
FROM renamed


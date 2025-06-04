WITH source AS (

    SELECT *
    FROM {{ ref('rspec_overall_time_source') }}

)


SELECT *
FROM source


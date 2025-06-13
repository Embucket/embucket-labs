WITH source AS (

    SELECT *
    FROM {{ref('xactly_position_hist_source')}}

)

SELECT *
FROM source
WITH source AS (

    SELECT *
    FROM {{ source('full_table_clones','mart_arr_rollup') }}

)

SELECT *
FROM source

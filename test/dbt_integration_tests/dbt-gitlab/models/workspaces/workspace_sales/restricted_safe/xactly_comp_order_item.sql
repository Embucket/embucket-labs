WITH source AS (

    SELECT *
    FROM {{ref('xactly_comp_order_item_source')}}

)

SELECT *
FROM source
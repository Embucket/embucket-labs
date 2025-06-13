WITH source AS (

    SELECT *
    FROM {{ref('xactly_comp_order_item_detail_source')}}

)

SELECT *
FROM source
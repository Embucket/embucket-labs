WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'net_arr_net_iacv_conversion_factors_agg') }}

), renamed AS (

    SELECT
      order_type_stamped::VARCHAR            AS order_type_stamped,
      user_segment::VARCHAR                  AS user_segment,
      net_iacv::NUMBER                       AS net_iacv,
      net_arr::NUMBER                        AS net_arr,
      ratio_net_iacv_to_net_arr::NUMBER      AS ratio_net_iacv_to_net_arr
    FROM source 

)

SELECT *
FROM renamed

WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'planned_values') }}

), renamed AS (

    SELECT
      unique_key::NUMBER                       AS primary_key,
      plan_month::DATE                         AS plan_month,
      planned_new_pipe::NUMBER                 AS planned_new_pipe,
      planned_total_iacv::NUMBER               AS planned_total_iacv,
      planned_tcv_minus_gross_opex::NUMBER     AS planned_tcv_minus_gross_opex,
      planned_total_arr::NUMBER                AS planned_total_arr,
      sales_efficiency_plan::FLOAT             AS sales_efficiency_plan,
      magic_number_plan::FLOAT                 AS magic_number_plan,
      tcv_plan::NUMBER                         AS tcv_plan,
      acv_plan::NUMBER                         AS acv_plan,
      planned_new_iacv::NUMBER                 AS planned_new_iacv,
      planned_growth_iacv::NUMBER              AS planned_growth_iacv,
      iacv_divided_by_capcon_plan::FLOAT       AS iacv_divided_by_capcon_plan,
      planned_iacv_ent_apac::NUMBER            AS planned_iacv_ent_apac,
      planned_iacv_ent_emea::NUMBER            AS planned_iacv_ent_emea,
      planned_iacv_ent_pubsec::NUMBER          AS planned_iacv_ent_pubsec,
      planned_iacv_ent_us_west::NUMBER         AS planned_iacv_ent_us_west,
      planned_iacv_ent_us_east::NUMBER         AS planned_iacv_ent_us_east,
      planned_iacv_mm::NUMBER                  AS planned_iacv_mm,
      planned_iacv_smb::NUMBER                 AS planned_iacv_sbm,
      planned_pio_ent_apac::NUMBER             AS planned_pio_ent_apac,
      planned_pio_ent_emea::NUMBER             AS planned_pio_ent_emea,
      planned_pio_ent_pubsec::NUMBER           AS planned_pio_ent_pubsec,
      planned_pio_ent_us_west::NUMBER          AS planned_pio_ent_us_west,
      planned_pio_ent_us_east::NUMBER          AS planned_pio_ent_us_east,
      planned_pio_mm::NUMBER                   AS planned_pio_mm,
      planned_pio_smb::NUMBER                  AS planned_pio_sbm,
      planned_pio_amer::NUMBER                 AS planned_pio_amer,
      planned_pio_emea::NUMBER                 AS planned_pio_emea,
      planned_pio_apac::NUMBER                 AS planned_pio_apac,
      planned_pio_pubsec::NUMBER               AS planned_pio_pubsec


    FROM source

)

SELECT *
FROM renamed

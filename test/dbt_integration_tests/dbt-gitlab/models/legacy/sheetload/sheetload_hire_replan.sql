WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_hire_replan_source') }}

), final AS (

    SELECT
      IFF(hp_departments='Brand and Digital Design', 'Brand & Digital Design',
            NULLIF(hp_departments, ''))::VARCHAR                AS department,
      hp_4_30_2020                                              AS "2020-04-30",
      hp_5_31_2020                                              AS "2020-05-31",
      hp_6_30_2020                                              AS "2020-06-30",
      hp_7_31_2020                                              AS "2020-07-31",
      hp_8_31_2020                                              AS "2020-08-31",
      hp_9_30_2020                                              AS "2020-09-30",
      hp_10_31_2020                                             AS "2020-10-31",
      hp_11_30_2020                                             AS "2020-11-30",
      hp_12_31_2020                                             AS "2020-12-31",
      hp_1_31_2021                                              AS "2021-01-31",
      hp_2_28_2021                                              AS "2021-02-28",
      hp_3_31_2021                                              AS "2021-03-31",
      hp_4_30_2021                                              AS "2021-04-30",
      hp_5_31_2021                                              AS "2021-05-31",
      hp_6_30_2021                                              AS "2021-06-30",
      hp_7_31_2021                                              AS "2021-07-31",
      hp_8_31_2021                                              AS "2021-08-31",
      hp_9_30_2021                                              AS "2021-09-30",
      hp_10_31_2021                                             AS "2021-10-31",
      hp_11_30_2021                                             AS "2021-11-30",
      hp_12_31_2021                                             AS "2021-12-31",
      hp_1_31_2022                                              AS "2022-01-31"
    FROM source

)

SELECT *
FROM final

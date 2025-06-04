WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'days_sales_outstanding') }}

), renamed AS (

  SELECT
    period::DATE                                                        AS period,
    regexp_replace(total_touch_billings, '[,]','')::NUMBER              AS total_touch_billings,
    regexp_replace(total_beginning_ar, '[,]','')::NUMBER                AS total_beginning_ar,
    regexp_replace(total_ar_at_end_of_period, '[,]','')::NUMBER         AS total_ar_at_end_of_period,
    nbr_of_days_in_period::NUMBER                                       AS nbr_of_days_in_period,
    dso::NUMBER                                                         AS dso,
    regexp_replace(collections_based_on_formula, '[,]','')::NUMBER      AS collections_based_on_formula,
    regexp_replace(total_current_ar_close, '[,]','')::NUMBER            AS total_current_ar_close,
    regexp_replace(p_d_ar, '[,]','')::NUMBER                            AS p_d_ar,
    collection_effectiveness_index::NUMBER                              AS collection_effectiveness_index,
    dso_trend::NUMBER                                                   AS dso_trend,
    cei_trend::NUMBER                                                   AS cei_trend
  FROM source

)

SELECT *
FROM renamed

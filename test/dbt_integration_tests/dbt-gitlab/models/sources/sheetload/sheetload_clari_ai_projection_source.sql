WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'clari_ai_projection') }}

), renamed AS (

    SELECT
      "Date"::DATE                                                  AS projection_date,
      "Projection_$"::NUMBER                                        AS projection_dollar_amount,
      "EOQ_Closed_$"::NUMBER                                        AS eoq_closed_dollar_amount,
      "Projection_Error_Percentage"::NUMBER                         AS projection_error_percentage,
      "Projection_Lower_Bound_$"::NUMBER                            AS projection_lower_bound_dollar_amount,
      "Projection_Upper_Bound_$"::NUMBER                            AS projection_upper_bound_dollar_amount,
      "Top_1_Source_of_Error"::VARCHAR                              AS top_one_source_of_error,
      "Top_1_Source_of_Error_$_Diff"::NUMBER                        AS top_one_source_of_error_dollar_amount_diff,
      "Top_2_Source_of_Error"::VARCHAR                              AS top_two_source_of_error,
      "Top_2_Source_of_Error_$_Diff"::NUMBER                        AS top_two_source_of_error_dollar_amount_diff,
      "Closed_as_of_Date_$"::NUMBER                                 AS closed_as_of_date_dollar_amount,
      "Closed_Converted_by_EOQ_$"::NUMBER                           AS closed_converted_by_eoq_dollar_amount,
      "Projected_Closed_Converted_by_EOQ_$"::NUMBER                 AS projected_closed_converted_by_eoq_dollar_amount,
      "Closed_Converted_by_EOQ_Percentage"::NUMBER                  AS closed_converted_by_eoq_percentage,
      "Projected_Closed_Converted_by_EOQ_Percentage"::NUMBER        AS projected_closed_converted_by_eoq_percentage,
      "New_as_of_Date_$"::NUMBER                                    AS new_as_of_date_dollar_amount,
      "New_Converted_by_EOQ_$"::NUMBER                              AS new_converted_by_eoq_dollar_amount,
      "Projected_New_Converted_by_EOQ_$"::NUMBER                    AS projected_new_converted_by_eoq_dollar_amount,
      "New_Converted_by_EOQ_Percentage"::NUMBER                     AS new_converted_by_eoq_percentage,
      "Projected_New_Converted_by_EOQ_Percentage"::NUMBER           AS projected_new_converted_by_eoq_percentage,
      "Best_Case_as_of_Date_$"::NUMBER                              AS best_case_as_of_date_dollar_amount,
      "Best_Case_Converted_by_EOQ_$"::NUMBER                        AS best_case_converted_by_eoq_dollar_amount,
      "Projected_Best_Case_Converted_by_EOQ_$"::NUMBER              AS projected_best_case_converted_by_eoq_dollar_amount,
      "Best_Case_Converted_by_EOQ_Percentage"::NUMBER               AS best_case_converted_by_eoq_percentage,
      "Projected_Best_Case_Converted_by_EOQ_Percentage"::NUMBER     AS projected_best_case_converted_by_eoq_percentage,
      "Commit_as_of_Date_$"::NUMBER                                 AS commit_as_of_date_dollar_amount,
      "Commit_Converted_by_EOQ_$"::NUMBER                           AS commit_converted_by_eoq_dollar_amount,
      "Projected_Commit_Converted_by_EOQ_$"::NUMBER                 AS projected_commit_converted_by_eoq_dollar_amount,
      "Commit_Converted_by_EOQ_Percentage"::NUMBER                  AS commit_converted_by_eoq_percentage,
      "Projected_Commit_Converted_by_EOQ_Percentage"::NUMBER        AS projected_commit_converted_by_eoq_percentage,
      "Omitted_as_of_Date_$"::NUMBER                                AS omitted_as_of_date_dollar_amount,
      "Omitted_Converted_by_EOQ_$"::NUMBER                          AS omitted_converted_by_eoq_dollar_amount,
      "Projected_Omitted_Converted_by_EOQ_$"::NUMBER                AS projected_omitted_converted_by_eoq_dollar_amount,
      "Omitted_Converted_by_EOQ_Percentage"::NUMBER                 AS omitted_converted_by_eoq_percentage,
      "Projected_Omitted_Converted_by_EOQ_Percentage"::NUMBER       AS projected_omitted_converted_by_eoq_percentage,
      "Pipeline_as_of_Date_$"::NUMBER                               AS pipeline_as_of_date_dollar_amount,
      "Pipeline_Converted_by_EOQ_$"::NUMBER                         AS pipeline_converted_by_eoq_dollar_amount,
      "Projected_Pipeline_Converted_by_EOQ_$"::NUMBER               AS projected_pipeline_converted_by_eoq_dollar_amount,
      "Pipeline_Converted_by_EOQ_Percentage"::NUMBER                AS pipeline_converted_by_eoq_percentage,
      "Projected_Pipeline_Converted_by_EOQ_Percentage"::NUMBER      AS projected_pipeline_converted_by_eoq_percentage
    FROM source
)


SELECT * 
FROM renamed
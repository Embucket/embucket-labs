WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_clari_ai_projection_source') }}

), final AS (

    SELECT 
      projection_date,
      projection_dollar_amount,
      eoq_closed_dollar_amount,
      projection_error_percentage,
      projection_lower_bound_dollar_amount,
      projection_upper_bound_dollar_amount,
      top_one_source_of_error,
      top_one_source_of_error_dollar_amount_diff,
      top_two_source_of_error,
      top_two_source_of_error_dollar_amount_diff,
      closed_as_of_date_dollar_amount,
      closed_converted_by_eoq_dollar_amount,
      projected_closed_converted_by_eoq_dollar_amount,
      closed_converted_by_eoq_percentage,
      projected_closed_converted_by_eoq_percentage,
      new_as_of_date_dollar_amount,
      new_converted_by_eoq_dollar_amount,
      projected_new_converted_by_eoq_dollar_amount,
      new_converted_by_eoq_percentage,
      projected_new_converted_by_eoq_percentage,
      best_case_as_of_date_dollar_amount,
      best_case_converted_by_eoq_dollar_amount,
      projected_best_case_converted_by_eoq_dollar_amount,
      best_case_converted_by_eoq_percentage,
      projected_best_case_converted_by_eoq_percentage,
      commit_as_of_date_dollar_amount,
      commit_converted_by_eoq_dollar_amount,
      projected_commit_converted_by_eoq_dollar_amount,
      commit_converted_by_eoq_percentage,
      projected_commit_converted_by_eoq_percentage,
      omitted_as_of_date_dollar_amount,
      omitted_converted_by_eoq_dollar_amount,
      projected_omitted_converted_by_eoq_dollar_amount,
      omitted_converted_by_eoq_percentage,
      projected_omitted_converted_by_eoq_percentage,
      pipeline_as_of_date_dollar_amount,
      pipeline_converted_by_eoq_dollar_amount,
      projected_pipeline_converted_by_eoq_dollar_amount,
      pipeline_converted_by_eoq_percentage,
      projected_pipeline_converted_by_eoq_percentage
    FROM source
    
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-11-09",
    updated_date="2020-11-09"
) }}

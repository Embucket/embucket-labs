WITH source AS (
    
    SELECT * 
    FROM {{ ref('categories_yaml_acquisitions_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category_name, category_stage, acquisition_key, acquisition_name, acquisition_end_date 
                               ORDER BY acquisition_start_date)=1

), final AS (

    SELECT
      category_name,
      category_stage,
      snapshot_date,
      acquisition_key,
      acquisition_name,
      acquisition_start_date,
      acquisition_end_date
    FROM source

)

SELECT *
FROM final
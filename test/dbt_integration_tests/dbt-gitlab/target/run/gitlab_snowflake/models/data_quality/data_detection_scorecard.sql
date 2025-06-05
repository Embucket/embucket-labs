
  
    

create or replace transient table EMBUCKET.data_quality.data_detection_scorecard
    

    
    as (WITH scorecard AS (

    SELECT 
      'Product Data Quality Scorecard'                                                                                                        AS scorecard_name,
      'The Dashboard displays the overall quality of Product Usage Data as measured by the status of individual Data Quality Detection Rules' AS scorecard_description,
      '2021-06-24'                                                                                                                            AS release_date,
      'V1.0'                                                                                                                                  AS version_number          
)

SELECT
      *,
      '@snalamaru'::VARCHAR       AS created_by,
      '@snalamaru'::VARCHAR       AS updated_by,
      '2021-07-20'::DATE        AS model_created_date,
      '2021-07-20'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM scorecard
    )
;


  

  
    

create or replace transient table EMBUCKET.legacy.sheetload_sdr_bdr_metric_targets
    

    
    as (WITH final AS (
SELECT *
FROM EMBUCKET.sheetload.sheetload_sdr_bdr_metric_targets_source

)

SELECT
      *,
      '@rkohnke'::VARCHAR       AS created_by,
      '@rkohnke'::VARCHAR       AS updated_by,
      '2022-06-16'::DATE        AS model_created_date,
      '2022-06-16'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final
    )
;


  
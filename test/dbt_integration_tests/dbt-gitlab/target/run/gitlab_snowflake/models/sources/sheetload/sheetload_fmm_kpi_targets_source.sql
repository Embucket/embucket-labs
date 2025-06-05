
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_fmm_kpi_targets_source
    

    
    as (WITH source AS (

    SELECT *
    FROM EMBUCKET.sheetload.fmm_kpi_targets

), renamed as (

    SELECT
      field_segment::VARCHAR                     AS field_segment,
      region::VARCHAR                            AS region,
      kpi::VARCHAR                               AS kpi,
      goal::NUMBER                               AS goal
    FROM source

)

SELECT *
FROM renamed
    )
;


  
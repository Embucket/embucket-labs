
  
    

create or replace transient table EMBUCKET.sheetload.sheetload_people_budget_source
    

    
    as (WITH source AS (

    SELECT * 
    FROM EMBUCKET.sheetload.people_budget
    
), renamed AS (

    SELECT
      "DIVISION"::VARCHAR                                  AS division,
      "FISCAL_YEAR"::NUMBER                                AS fiscal_year,
      "QUARTER"::NUMBER                                    AS fiscal_quarter,
      "BUDGET"::NUMBER                                     AS budget,
      "EXCESS_FROM_PREVIOUS_QUARTER"::NUMBER               AS excess_from_previous_quarter,
      "ANNUAL_COMP_REVIEW"::NUMBER                         AS annual_comp_review
    FROM source
 
)

SELECT *
FROM renamed
    )
;


  
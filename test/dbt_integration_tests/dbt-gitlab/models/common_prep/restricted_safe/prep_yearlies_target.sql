{{ config(materialized='view') }}

WITH prep as (
  
 SELECT 
  yearly_name,
  yearly_dri,
  yearly_description,
  is_mnpi,
  IFNULL(FY25_Q4,-1) AS FY25_Q4,
  IFNULL(FY25_Q3,-1) AS FY25_Q3,
  IFNULL(FY25_Q2,-1) AS FY25_Q2,
  IFNULL(FY25_Q1,-1) AS FY25_Q1

FROM
  {{ ref('sheetload_fy25_yearlies_target_source') }}
),
final AS (

SELECT
   yearly_name,
   yearly_dri,
   yearly_description,
   is_mnpi,
   REPLACE(quarter_name, '_', '-') AS quarter,
   IFF(target = -1, NULL, target) AS targets_raw
FROM
   prep UNPIVOT(target FOR quarter_name IN 
   (
      "FY25_Q4",
      "FY25_Q3",
      "FY25_Q2",
      "FY25_Q1"
   )
))

SELECT * 
FROM final
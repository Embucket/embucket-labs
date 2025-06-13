WITH source AS (

  SELECT * 
  FROM {{ ref('sheetload_books_source') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-05-26",
    updated_date="2022-05-26"
) }}
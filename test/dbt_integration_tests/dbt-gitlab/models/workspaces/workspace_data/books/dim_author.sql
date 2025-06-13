WITH source AS (

  SELECT DISTINCT
    --Primary Key
      {{ dbt_utils.generate_surrogate_key(['authors']) }} AS dim_author_id,
      authors AS author_name
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2022-06-02",
    updated_date="2022-06-06"
) }}

WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.generate_surrogate_key(['bookid', 'isbn13']) }} AS dim_book_id,
      title AS book_title, 
      language_code AS language,
      isbn, 
      num_pages AS pages_count
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@lisvinueza",
    updated_by="@lisvinueza",
    created_date="2022-06-02",
    updated_date="2022-06-06"
) }}

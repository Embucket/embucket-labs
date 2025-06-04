WITH source AS (

  SELECT 
    --Primary Key
      {{ dbt_utils.generate_surrogate_key(['bookid', 'publication_date', 'authors', 'isbn13', 'publisher']) }} AS dim_book_rating_id,
    
    --Foreign Keys
      {{ dbt_utils.generate_surrogate_key(['bookid', 'isbn13']) }} AS dim_book_id,
      {{ dbt_utils.generate_surrogate_key(['authors']) }} AS dim_author_id,
      {{ dbt_utils.generate_surrogate_key(['publisher']) }} AS dim_publisher_id,
      {{ get_date_id('publication_date') }} AS dim_publication_date_id, 


    
    --Measures
      text_reviews_count, 
      average_rating, 
      ratings_count
  FROM {{ ref('sheetload_books') }}

)

{{ dbt_audit(
    cte_ref="source",
    created_by="@michellecooper",
    updated_by="@lisvinueza",
    created_date="2022-06-01",
    updated_date="2022-06-06"
) }}

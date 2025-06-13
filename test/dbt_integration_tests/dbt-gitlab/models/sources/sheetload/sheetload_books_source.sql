WITH source AS (

  SELECT 
      'bookID' AS bookid,
      title,
      authors,
      average_rating,
      isbn,
      isbn13, 
      language_code,
      num_pages,
      ratings_count,
      text_reviews_count,
      publication_date,
      publisher
  FROM {{ source('sheetload','books') }}

)
SELECT * 
FROM source

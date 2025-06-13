WITH source AS (

  SELECT * 
  FROM {{ source('driveload', 'marketing_press_sov') }}

), renamed AS (

    SELECT
      "Brand"::VARCHAR                                          AS brand,
      "Date"::DATE                                              AS date,
      "Media_Type"::VARCHAR                                     AS media_type,
      "Media_Outlet"::VARCHAR                                   AS media_outlet,
      "Title"::VARCHAR                                          AS title,
      "Link"::VARCHAR                                           AS link,
      "Author"::VARCHAR                                         AS author,
      "Sentiment"::VARCHAR                                      AS sentiment,
      "Circulation"::VARCHAR                                    AS circulation,
      REPLACE("Desktop_Readership", ',')::NUMBER                AS desktop_readership,
      TRIM(REPLACE("Mobile_Readership", ','), '$')::NUMBER      AS mobile_readership, 
      REPLACE("Total_Readership", ',')::NUMBER                  AS total_readership, 
      REPLACE("Local_Viewership", ',')::VARCHAR                 AS local_viewership, 
      REPLACE("National_Viewership", ',')::NUMBER               AS national_viewership, 
      REPLACE("Shares", ',')::NUMBER                            AS shares, 
      TRIM(REPLACE("Ad_Equivalency", ','), '$')::DECIMAL(10,2)  AS ad_equivalency_usd, 
      "Article_Impact"::VARCHAR                                 AS article_impact, 
      "SEO_Impact"::NUMBER                                      AS seo_impact, 
      "Tags"::VARCHAR                                           AS tags, 
      "Country"::VARCHAR                                        AS country, 
      "State"::VARCHAR                                          AS state, 
      "City"::VARCHAR                                           AS city
    FROM source

)

SELECT * 
FROM renamed
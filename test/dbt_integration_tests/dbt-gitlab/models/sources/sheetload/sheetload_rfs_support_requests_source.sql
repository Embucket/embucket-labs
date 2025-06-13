WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','rfs_support_requests') }}

), final AS (
    
    SELECT 
      NULLIF(customer_prospect_name, '')::VARCHAR                       AS customer_prospect_name,
      NULLIF(request_type, '')::VARCHAR                                 AS request_type,
      NULLIF(market_industry_vertical, '')::VARCHAR                     AS market_industry_vertical,
      NULLIF(customer_prospect_size, '')::VARCHAR                       AS customer_prospect_size,
      NULLIF(sfdc_link, '')::VARCHAR                                    AS sfdc_link,
      NULLIF(iacv_impact, '')::VARCHAR                                  AS iacv_impact,
      NULLIF(product_host, '')::VARCHAR                                 AS product_host,
      NULLIF(due_date, '')::VARCHAR                                     AS due_date,
      NULLIF(other, '')::VARCHAR                                        AS other,
      NULLIF(requestor_name, '')::VARCHAR                               AS requestor_name,
      NULLIF(additional_gitlab_team_members, '')::VARCHAR               AS additional_gitlab_team_members,
      NULLIF(month,'')::DATE                                            AS date
    FROM source

) 

SELECT * 
FROM final

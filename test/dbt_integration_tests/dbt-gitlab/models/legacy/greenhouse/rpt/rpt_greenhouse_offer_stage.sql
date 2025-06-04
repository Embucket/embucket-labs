WITH recruiting_xf AS (
    
    SELECT * 
    FROM {{ref('greenhouse_recruiting_xf')}} 
    WHERE offer_id IS NOT NULL
    AND offer_status <>'rejected'
  
), greenhouse_offer_custom_fields as (

    SELECT 
        offer_id, 
        offer_custom_field,
        offer_custom_field_display_value                                                    AS candidate_country
    FROM {{ref('greenhouse_offer_custom_fields_source')}}
    WHERE offer_custom_field = 'Candidate Country'

), zuora_regions AS (

    SELECT *
    FROM {{ref('zuora_country_geographic_region')}}

), bamboohr AS (

    SELECT 
      greenhouse_candidate_id, 
      IFF(region = 'JAPAC','APJ', region) AS region
    FROM  {{ref('bamboohr_id_employee_number_mapping')}} 

), location_cleaned AS (

    SELECT
      offer_id,
      candidate_country,
      IFF(LOWER(LEFT(candidate_country,12))= 'united state', 
          'North America',
          COALESCE(z1.geographic_region,z2.geographic_region, z3.geographic_region, candidate_country))       AS geographic_region      
    FROM greenhouse_offer_custom_fields
    LEFT JOIN zuora_regions z1 
      ON LOWER(z1.country_name_in_zuora) = LOWER(greenhouse_offer_custom_fields.candidate_country)
    LEFT JOIN zuora_regions z2 
      ON LOWER(z2.iso_alpha_2_code) = LOWER(greenhouse_offer_custom_fields.candidate_country)
    LEFT JOIN zuora_regions z3 
      ON LOWER(z3.iso_alpha_3_code) = LOWER(greenhouse_offer_custom_fields.candidate_country) 

), data_set AS (

    SELECT 
      recruiting_xf.offer_id,
      application_status, 
      current_stage_name                               AS stage_name, 
      offer_status,
      offer_sent_date,
      offer_resolved_date,
      candidate_target_hire_date                        AS start_date,
      candidate_country,
      geographic_region,
      bamboohr.region as bh_region,
      CASE WHEN geographic_region IN ('North America', 'South America') 
            THEN geographic_region
           ELSE COALESCE(bh_region, geographic_region) END AS region_final
    FROM recruiting_xf
    INNER JOIN bamboohr
      ON recruiting_xf.candidate_id = bamboohr.greenhouse_candidate_id
    INNER JOIN location_cleaned
      ON location_cleaned.offer_id = recruiting_xf.offer_id  

), final AS (

    SELECT 
      DATE_TRUNC(WEEK,start_date)                               AS start_week,
      region_final                                              AS geographic_region,
      COUNT(offer_id)                                           AS candidates_estimated_to_start,
      SUM(IFF(offer_status = 'accepted',1,0))                   AS accepted_offers_to_start
    FROM data_set
    WHERE geographic_region IN ('North America', 'South America', 'EMEA','APJ', 'Americas')
    GROUP BY 1,2
    ORDER BY 1 DESC

)

SELECT * 
FROM final

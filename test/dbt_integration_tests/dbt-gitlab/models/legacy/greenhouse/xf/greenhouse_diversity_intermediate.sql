{% set repeated_column_names = 
    "greenhouse_recruiting_xf.application_id,
      department_name::VARCHAR(100)                                                 AS department_name,
      division::VARCHAR(100)                                                        AS division,
      division_modified::VARCHAR(100)                                               AS division_modified,
      source_type::VARCHAR(100)                                                     AS source_type,
      CASE WHEN eeoc_values in ('I don''t wish to answer','Decline To Self Identify') 
            THEN 'did not identify'
           WHEN eeoc_values = 'No, I don''t have a disability' 
            THEN 'No' 
            ELSE COALESCE(lower(eeoc_values), 'did not identify') end                AS eeoc_values
" %}

WITH date_details AS (
  
    SELECT 
      date_actual                                                          AS month_date,                               
      'join'                                                               AS join_field  
    FROM {{ ref ('date_details') }}
    WHERE date_actual <= {{max_date_in_bamboo_analyses()}}
      AND day_of_month = 1 
      AND date_actual >= '2018-08-12' -- 1st date we started capturing eeoc data

), greenhouse_recruiting_xf AS (

    SELECT *
    FROM  {{ ref ('greenhouse_recruiting_xf') }}

), eeoc AS (

      {{ dbt_utils.unpivot(
      relation=ref('greenhouse_eeoc_responses_source'),
      cast_to='varchar',
      exclude=['application_id'],
      remove=['eeoc_response_submitted_at'],
      field_name='eeoc_field_name',
      value_name='eeoc_values'
      ) }}

), eeoc_fields AS (

    SELECT DISTINCT 
      LOWER(eeoc_field_name)::VARCHAR(100)             AS eeoc_field_name,
      'join'                                           AS join_field
    FROM eeoc

), base AS (

    SELECT
      month_date,
      eeoc_field_name
    FROM date_details
    LEFT JOIN eeoc_fields 
      ON eeoc_fields.join_field = date_details.join_field  
  
    UNION ALL

    SELECT
      month_date,
      'no_eeoc' AS eeoc_field_name
    FROM date_details
    

), applications AS (

    SELECT 
      base.*,
      'application_month'                                                               AS capture_month,
      {{repeated_column_names}},
      IFF(offer_status = 'accepted',1,0)                                                AS accepted_offer,
      null                                                                              AS time_to_offer,
      IFF(sourced_candidate = TRUE, 1,0)                                                AS sourced_candidate,
      IFF(sourced_candidate = TRUE AND offer_status = 'accepted', 1,0)                  AS hired_sourced_candidate
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month',greenhouse_recruiting_xf.application_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name  


), offers AS (

    SELECT 
      base.*,
      'offer_sent_month'                                                               AS capture_month,
      {{repeated_column_names}},
      IFF(offer_status = 'accepted',1,0)                                                AS accepted_offer,
      null                                                                              AS time_to_offer,
      IFF(sourced_candidate = TRUE, 1,0)                                                AS sourced_candidate,
      IFF(sourced_candidate = TRUE AND offer_status = 'accepted', 1,0)                  AS hired_sourced_candidate
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month',greenhouse_recruiting_xf.offer_sent_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name 
    WHERE offer_status IS NOT NULL
 
), accepted AS (

    SELECT 
      base.*,
      'accepted_month'                                                                  AS capture_month,
      {{repeated_column_names}},
      IFF(offer_status = 'accepted',1,0)                                                AS accepted_offer,
      time_to_offer,
      IFF(sourced_candidate = TRUE, 1,0)                                                AS sourced_candidate,
      IFF(sourced_candidate = TRUE AND offer_status = 'accepted', 1,0)                  AS hired_sourced_candidate                           
    FROM base
    LEFT JOIN greenhouse_recruiting_xf
      ON DATE_TRUNC('month',greenhouse_recruiting_xf.offer_resolved_date) = base.month_date
    LEFT JOIN eeoc            
      ON greenhouse_recruiting_xf.application_id = eeoc.application_id
      AND LOWER(eeoc.eeoc_field_name) = base.eeoc_field_name 
    WHERE base.month_date >= '2018-09-01' -- 1st date we started capturing eeoc data
      AND offer_status ='accepted'

), final AS (

    SELECT * 
    FROM applications

    UNION ALL

    SELECT * 
    FROM offers

    UNION ALL

    SELECT *
    FROM accepted 

) 

SELECT * 
FROM final  
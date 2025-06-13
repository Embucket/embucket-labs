WITH applications AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_source') }}

), referrer AS (

    SELECT *
    FROM  {{ ref ('greenhouse_referrers_source') }}

), source AS (

    SELECT *
    FROM  {{ ref ('greenhouse_sources_source') }}


), candidate AS (

    SELECT *
    FROM  {{ ref ('greenhouse_candidates_source') }}

), intermediate AS (

    SELECT 
      application_id,
      applications.candidate_id,
      applications.referrer_id,
      referrer_name         AS sourcer_name,
      applied_at            AS application_date, 
      candidate_created_at
    FROM applications
    LEFT JOIN referrer
      ON applications.referrer_id = referrer.referrer_id
    LEFT JOIN source  
      ON applications.source_Id = source.source_id
    LEFT JOIN candidate
      ON applications.candidate_id =  candidate.candidate_id 
    WHERE source.source_type = 'Prospecting'  
  
)

SELECT * 
FROM intermediate




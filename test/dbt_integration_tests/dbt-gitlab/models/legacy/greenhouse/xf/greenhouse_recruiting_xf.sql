WITH applications AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_source') }}
    WHERE applied_at >='2017-01-01'

), offers AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_offers_source') }}

), greenhouse_application_jobs AS (

    SELECT *
    FROM  {{ ref ('greenhouse_applications_jobs_source') }}

), jobs AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_jobs_source') }}

), job_req AS (

    SELECT 
      applications.application_id, 
      jobs.*,
      COUNT(jobs.job_id) OVER (PARTITION BY applications.application_id) AS total_reqs_for_job_id,
      CASE WHEN total_reqs_for_job_id = 1 
             THEN 1 
           WHEN total_reqs_for_job_id>1
             AND applications.applied_at BETWEEN jobs.job_created_at 
             AND COALESCE(jobs.job_closed_at, DATEADD(week, 3, CURRENT_DATE()))
             THEN 1
           ELSE 0 END                                                    AS job_req_to_use 
    FROM applications
    LEFT JOIN greenhouse_application_jobs 
      ON applications.application_id = greenhouse_application_jobs.application_id
    LEFT JOIN jobs 
      ON greenhouse_application_jobs.job_id = jobs.job_id
    
), greenhouse_departments AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_departments_source') }}

), greenhouse_sources AS (

    SELECT * 
    FROM  {{ ref ('greenhouse_sources_source') }}

), greenhouse_sourcer AS (

    SELECT * 
    FROM {{ ref ('greenhouse_sourcer') }}

), rejection_reasons AS (
    
    SELECT * 
    FROM {{ ref ('greenhouse_rejection_reasons_source') }}

), cost_center AS (

    SELECT DISTINCT division, department
    FROM {{ref('cost_center_division_department_mapping_current')}}

), bamboo AS (

    SELECT greenhouse_candidate_id, hire_date 
    FROM {{ref('bamboohr_id_employee_number_mapping')}}
    WHERE greenhouse_candidate_id IS NOT NULL

), final AS (

    SELECT 
      {{ dbt_utils.generate_surrogate_key(['applications.application_id',
                                  'offers.offer_id',
                                  'applications.candidate_id',
                                  'job_req.job_id',
                                  'job_req.requisition_id']) }}                         AS unique_key,
        applications.application_id, 
        offers.offer_id,
        applications.candidate_id, 
        job_req.job_id,
        job_req.requisition_id, 
        applications.prospect                                                           AS is_prospect,
        applications.application_status,
        applications.stage_name                                                         AS current_stage_name, 
        offers.offer_status,
        applications.applied_at                                                         AS application_date,
        offers.sent_at                                                                  AS offer_sent_date,
        offers.resolved_at                                                              AS offer_resolved_date,
        offers.start_date                                                               AS candidate_target_hire_date,
        applications.rejected_at                                                        AS rejected_date,
        job_req.job_name,
        greenhouse_departments.department_name                                          AS department_name,
        cost_center.division                                                            AS division,                                             
        CASE WHEN lower(greenhouse_departments.department_name) LIKE '%sales%' 
               THEN 'Sales'
             WHEN greenhouse_departments.department_name = 'Dev' 
               THEN 'Engineering'
             WHEN greenhouse_departments.department_name = 'Customer Success Management' 
               THEN 'Sales'
             ELSE COALESCE(cost_center.division, 
                    greenhouse_departments.department_name) END                         AS division_modified,     
        greenhouse_sources.source_name                                                  AS source_name,
        greenhouse_sources.source_type                                                  AS source_type,
        CASE WHEN TRIM(source_name) IN ('Sales Bootcamp','Social media presence','Greenhouse','Maildrop','Reddit',
                              'Slack Groups','AmazingHiring','AngelList','Google','Greenhouse Sourcing',
                              'LinkedIn (Prospecting)','SocialReferral','Talent Community','Viren - LinkedIn',
                              'Referral', 'LinkedIn (Social Media)','Twitter') THEN 1
            WHEN TRIM(source_type) = 'In person event' THEN 1 ELSE 0 END                AS is_outbound,
        CASE WHEN TRIM(source_name) IN ('Greenhouse','Maildrop','Reddit','Slack Groups','AmazingHiring','Google',
                             'Greenhouse Sourcing','LinkedIn (Prospecting)','Talent Community',
                             'Viren - LinkedIn','LinkedIn (Social Media)','Twitter') THEN 1
            WHEN TRIM(source_type) = 'In person event' THEN 1 ELSE 0 END                AS is_sourced,
        greenhouse_sourcer.sourcer_name,
        applications.application_recruiter,
        applications.application_coordinator,
        IFF(greenhouse_sources.source_name ='LinkedIn (Prospecting)',True, False)       AS sourced_candidate,
        rejection_reasons.rejection_reason_name,
        rejection_reasons.rejection_reason_type,
        job_req.job_status                                                              AS current_job_req_status,
        IFF(offers.offer_status ='accepted',
                DATEDIFF('day', applications.applied_at, offers.resolved_at),
                NULL)                                                                   AS time_to_offer,
        IFF(bamboo.hire_date IS NOT NULL, TRUE, FALSE)                                  AS is_hired_in_bamboo
    FROM applications 
    LEFT JOIN job_req
      ON job_req.application_id = applications.application_id
      AND job_req.job_req_to_use = 1 
    LEFT JOIN  greenhouse_departments 
      ON job_req.department_id = greenhouse_departments.department_id
      AND job_req.organization_id = greenhouse_departments.organization_id
    LEFT JOIN greenhouse_sources 
      ON greenhouse_sources.source_id = applications.source_id 
    LEFT JOIN offers 
      ON applications.application_id = offers.application_id
    LEFT JOIN greenhouse_sourcer
      ON applications.application_id = greenhouse_sourcer.application_id
    LEFT JOIN rejection_reasons
      ON rejection_reasons.rejection_reason_id = applications.rejection_reason_id
    LEFT JOIN cost_center
      ON TRIM(greenhouse_departments.department_name)=TRIM(cost_center.department)
    LEFT JOIN bamboo
      ON bamboo.greenhouse_candidate_id = applications.candidate_id

)

SELECT *
FROM final 



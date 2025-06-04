{% set repeated_column_names = 
        "job_id,
        requisition_id,
        is_prospect,
        current_stage_name,
        application_status,
        job_name,
        department_name,
        division_modified,
        source_name,
        source_type,
        sourcer_name,
        is_outbound,
        is_sourced,
        rejection_reason_name,
        rejection_reason_type,
        current_job_req_status,
        is_hired_in_bamboo,
        time_to_offer" %}


WITH stages AS (

    SELECT *
    FROM {{ ref ('greenhouse_application_stages_source') }}
    WHERE stage_entered_on IS NOT NULL
    
), stages_pivoted AS (

    SELECT 
      application_id,
      null as application_review,
      null as screen,
      null as team_interview,
      null as background_check_and_offer,
      {{ dbt_utils.pivot(
          'stage_name_modified_with_underscores', 
          dbt_utils.get_column_values(ref('greenhouse_application_stages_source'), 'stage_name_modified_with_underscores'),
          agg = 'MAX',
          then_value = 'stage_entered_on',
          else_value = 'NULL',
          quote_identifiers = TRUE
      ) }}
    FROM {{ref('greenhouse_application_stages_source')}}
    GROUP BY application_id

), recruiting_xf AS (

    SELECT * 
    FROM {{ ref ('greenhouse_recruiting_xf') }}

), hires_data AS (

    SELECT
      application_id,
      candidate_id,
      hire_date_mod
    FROM {{ ref ('greenhouse_hires') }}

), applications AS (

    SELECT 
        application_id,
        candidate_id,
        'Application Submitted'                                                         AS application_stage,
        TRUE                                                                            AS is_milestone_stage,
        DATE_TRUNC(MONTH, application_date)                                             AS application_month,
        application_date                                                                AS stage_entered_on,
        null                                                                            AS stage_exited_on,
    {{repeated_column_names}}
    FROM recruiting_xf 

), stages_intermediate AS (
    
    SELECT 
      stages.application_id,
      candidate_id,
      stages.stage_name_modified                                                      AS application_stage,
      stages.is_milestone_stage,
      DATE_TRUNC(MONTH, application_date)                                             AS application_month,
      IFF(application_stage_name = 'Background Check and Offer',offer_sent_date, stages.stage_entered_on)  AS stage_entered_on,
      IFF(application_stage_name = 'Background Check and Offer', offer_resolved_date, 
            COALESCE(stages.stage_exited_on, CURRENT_DATE()))                         AS stage_exited_on,
      {{repeated_column_names}}
    FROM stages
    LEFT JOIN recruiting_xf 
      ON recruiting_xf.application_id = stages.application_id

), hired AS (

    SELECT 
      hires_data.application_id,
      hires_data.candidate_id,
      'Hired'                                                                         AS application_stage,
      TRUE                                                                            AS is_milestone_stage,
      DATE_TRUNC(MONTH, application_date)                                             AS application_month,
      hire_date_mod                                                                   AS stage_entered_on,
      hire_date_mod                                                                   AS stage_exited_on,
      {{repeated_column_names}}
    FROM  hires_data
    LEFT JOIN recruiting_xf 
      ON recruiting_xf.application_id = hires_data.application_id

), rejected AS (

    SELECT 
      application_id,
      candidate_id,
      'Rejected'                                                                      AS application_stage,
      TRUE                                                                            AS is_milestone_stage,
      DATE_TRUNC(MONTH, application_date)                                             AS application_month,
      rejected_date                                                                   AS stage_entered_on,
      rejected_date                                                                   AS stage_exited_on,
      {{repeated_column_names}}
    FROM recruiting_xf 
    WHERE application_status in ('rejected')
    
), all_stages AS (

    SELECT * 
    FROM applications 

    UNION ALL
    
    SELECT * 
    FROM stages_intermediate
    
    UNION ALL

    SELECT *
    FROM hired

    UNION ALL

    SELECT *
    FROM rejected

), stages_hit AS (

    SELECT 
    application_id,
    candidate_id,
    MIN(stage_entered_on)                                                       AS min_stage_entered_on,
    MAX(stage_exited_on)                                                        AS max_stage_exited_on,
    SUM(IFF(application_stage = 'Application Submitted',1,0))                   AS hit_application_submitted,
    SUM(IFF(application_stage = 'Application Review',1,0))                      AS hit_application_review,
    SUM(IFF(application_stage = 'Assessment',1,0))                              AS hit_assessment,
    SUM(IFF(application_stage = 'Screen',1,0))                                  AS hit_screening,
    SUM(IFF(application_stage = 'Team Interview - Face to Face',1,0))           AS hit_team_interview,
    SUM(IFF(application_stage = 'Reference Check',1,0))                         AS hit_reference_check,
    SUM(IFF(application_stage = 'Background Check and Offer',1,0))              AS hit_offer,
    SUM(IFF(application_stage = 'Hired',1,0))                                   AS hit_hired,
    SUM(IFF(application_stage = 'Rejected',1,0))                                AS hit_rejected
    FROM all_stages
    GROUP BY 1,2
        
), intermediate AS (

    SELECT 
        all_stages.*,
        ROW_NUMBER() OVER (PARTITION BY application_id, candidate_id 
                            ORDER BY stage_entered_on DESC)                       AS row_number_stages_desc
    FROM all_stages        

), stage_order_revamped AS (

    SELECT
        intermediate.*,
        CASE WHEN application_stage in ('Hired','Rejected') AND (hit_rejected = 1 or hit_hired = 1 )       
                THEN 1
            WHEN (hit_rejected = 1 or hit_hired = 1 )   
                THEN   row_number_stages_desc+1
            ELSE row_number_stages_desc END             AS row_number_stages_desc_updated
    FROM intermediate 
    LEFT JOIN stages_hit
        ON intermediate.application_id = stages_hit.application_id
        AND intermediate.candidate_id = stages_hit.candidate_id

), final AS (   

    SELECT
        {{ dbt_utils.generate_surrogate_key(['stage_order_revamped.application_id', 'stage_order_revamped.candidate_id']) }} AS unique_key,
        stage_order_revamped.application_id,
        stage_order_revamped.candidate_id,
        application_stage, 
        is_milestone_stage,
        stage_entered_on,
        stage_exited_on,
        LEAD(application_stage) OVER 
            (PARTITION BY stage_order_revamped.application_id, stage_order_revamped.candidate_id 
                ORDER BY row_number_stages_desc_updated DESC)                       AS next_stage,
        LEAD(stage_entered_on) OVER 
            (PARTITION BY stage_order_revamped.application_id, stage_order_revamped.candidate_id 
                ORDER BY row_number_stages_desc DESC)                               AS next_stage_entered_on,                       
        DATE_TRUNC(MONTH,stage_entered_on)                                          AS month_stage_entered_on,
        DATE_TRUNC(MONTH,stage_exited_on)                                           AS month_stage_exited_on,
        DATEDIFF(DAY, stage_entered_on, COALESCE(stage_exited_on, CURRENT_DATE()))  AS days_in_stage,
        DATEDIFF(DAY, stage_entered_on, 
            COALESCE(next_stage_entered_on, CURRENT_DATE()))                        AS days_between_stages,
        DATEDIFF(DAY, min_stage_entered_on, max_stage_exited_on)                    AS days_in_pipeline,
        row_number_stages_desc_updated                                              AS row_number_stages_desc,        
        IFF(row_number_stages_desc_updated = 1, TRUE, FALSE)                        AS is_current_stage,

        application_month,
        {{repeated_column_names}},
        hit_application_review,
        hit_assessment,
        hit_screening,
        hit_team_interview,
        hit_reference_check,
        hit_offer,
        hit_hired,
        hit_rejected,
        IFF(hit_team_interview = 0 
              AND hit_rejected =1 
              AND rejection_reason_type = 'They rejected us',1,0)                   AS candidate_dropout,
        CASE WHEN is_current_stage = True
                AND application_stage NOT IN ('Hired','Rejected')
                AND hit_rejected = 0
                AND hit_hired = 0
                AND current_job_req_status = 'open'
                AND application_status = 'active' 
              THEN TRUE 
              ELSE FALSE END                                                        AS in_current_pipeline,
        DATEDIFF(day, stages_pivoted.application_review, stages_pivoted.screen)     AS turn_time_app_review_to_screen,
        DATEDIFF(day, stages_pivoted.screen, stages_pivoted.team_interview    )     AS turn_time_screen_to_interview,
        DATEDIFF(day, stages_pivoted.team_interview, stages_pivoted.background_check_and_offer)          AS turn_time_interview_to_offer
    FROM stage_order_revamped
    LEFT JOIN stages_hit 
        ON stage_order_revamped.application_id = stages_hit.application_id
        AND stage_order_revamped.candidate_id = stages_hit.candidate_id
    LEFT JOIN hires_data 
      ON stage_order_revamped.application_id = hires_data.application_id
      AND stage_order_revamped.candidate_id = hires_data.candidate_id
    LEFT JOIN stages_pivoted
      ON stages_pivoted.application_id = stage_order_revamped.application_id 
    
)
        
SELECT *      
FROM final

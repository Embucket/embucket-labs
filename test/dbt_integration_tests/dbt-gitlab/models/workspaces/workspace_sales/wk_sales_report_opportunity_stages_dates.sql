{{ config(
    tags=["mnpi_exception"]
) }}

 {{ config(alias='report_opportunity_stages_dates') }}

WITH sfdc_opportunity_field_history AS (

  SELECT *
  FROM {{ ref('sfdc_opportunity_field_history_source')}}

), date_details AS (

    SELECT * 
    FROM {{ ref('wk_sales_date_details') }} 

), sfdc_opportunity_xf AS (

  SELECT *
  FROM {{ ref('wk_sales_sfdc_opportunity_xf')}}

-- after every stage change, as it is a tracked field
-- a record would be created in the field history table
), new_history_base AS (
    SELECT opportunity_id,
           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(new_value_string, '2-Developing', '2-Scoping'), '7 - Closing',
                                           '7-Closing'), 'Developing', '2-Scoping'), 'Closed Lost', '8-Closed Lost'),
                   '8-8-Closed Lost', '8-Closed Lost') AS new_value_string,
         -- MIN as we want to track the lowest possible case when the stage showed up.
           MIN(field_modified_at::date)                AS min_stage_date

    FROM sfdc_opportunity_field_history
    WHERE opportunity_field = 'stagename'
    GROUP BY 1, 2

), old_history_base AS (

    --NF: Two used cases of this CTE:
    -- NF: Trying to add back opportunities that were created in a different stage than 1
    -- NF: Identify skipped deals
    SELECT hist.opportunity_id,
           REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(hist.old_value_string, '2-Developing', '2-Scoping'),
                                                   '7 - Closing', '7-Closing'), 'Developing', '2-Scoping'),
                                   'Closed Lost', '8-Closed Lost'), '8-8-Closed Lost', '8-Closed Lost'),
                   '00-Pre Opportunity', '0-Pending Acceptance')        AS stage_name,
           hist.field_modified_at::date                                 AS field_modified,
           oppty.created_date
    FROM sfdc_opportunity_field_history hist
    INNER JOIN sfdc_opportunity_xf oppty
        ON hist.opportunity_id = oppty.opportunity_id
    WHERE opportunity_field = 'stagename'
      AND old_value_string != '00-Pre Opportunity'
    UNION
    --This accounts for the created scenario
    SELECT opportunity_id,
           stage_name,
           created_date AS field_modified,
           created_date
    FROM sfdc_opportunity_xf
        WHERE opportunity_id NOT IN (SELECT DISTINCT opportunity_id
                                    FROM sfdc_opportunity_field_history
                                    WHERE opportunity_field = 'stagename'
                                        AND old_value_string != '00-Pre Opportunity')
    UNION
    --This accounts for the current state, needed to identify skips
    -- it will only trigger if the opportunity has a history
    SELECT opportunity_id,
           stage_name,
           CURRENT_DATE AS field_modified,
           created_date
    FROM sfdc_opportunity_xf
    WHERE opportunity_id IN (SELECT DISTINCT opportunity_id
                                 FROM sfdc_opportunity_field_history
                                 WHERE opportunity_field = 'stagename'
                                    AND old_value_string != '00-Pre Opportunity')

), old_history_base_rank AS (
    --NF: We select #1 to account for created status
    SELECT old.*,
           ROW_NUMBER() OVER (PARTITION BY opportunity_id ORDER BY field_modified) AS rank_stage
    FROM old_history_base old
    QUALIFY rank_stage = 1

), not_skipped AS (
-- to asses if a deal was skipped we leverage the old stage value
-- if a stage never shows up on an old stage value at the stage date it was skipped

    SELECT
           opportunity_id,
           MAX(CASE WHEN stage_name = '0-Pending Acceptance'
                THEN 1
            ELSE 0 END)                  AS not_skipped_stage_0_flag,
           MAX(CASE WHEN stage_name = '1-Discovery'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_1_flag,
           MAX(CASE WHEN stage_name = '2-Scoping'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_2_flag,
           MAX(CASE WHEN stage_name = '3-Technical Evaluation'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_3_flag,
           MAX(CASE WHEN stage_name = '4-Proposal'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_4_flag,
           MAX(CASE WHEN stage_name = '5-Negotiating'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_5_flag,
           MAX(CASE WHEN stage_name = '6-Awaiting Signature'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_6_flag,
           MAX(CASE WHEN stage_name = '7-Closing'
              THEN 1
           ELSE 0 END)                  AS not_skipped_stage_7_flag
    FROM old_history_base
    GROUP BY 1

), combined AS (

 -- add opportunities changed after creation
  SELECT opportunity_id,
      new_value_string AS stage_name,
      min_stage_date
  FROM new_history_base
  UNION
  -- add opportunities created on stages above 0
  SELECT opportunity_id,
      stage_name,
      created_date
  FROM old_history_base_rank

), pivoted_combined AS (

  SELECT opportunity_id,
       MIN(CASE WHEN stage_name = '0-Pending Acceptance'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_0_date,
       MIN(CASE WHEN stage_name = '1-Discovery'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_1_date,
       MIN(CASE WHEN stage_name = '2-Scoping'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_2_date,
       MIN(CASE WHEN stage_name = '3-Technical Evaluation'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_3_date,
       MIN(CASE WHEN stage_name = '4-Proposal'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_4_date,
       MIN(CASE WHEN stage_name = '5-Negotiating'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_5_date,
       MIN(CASE WHEN stage_name = '6-Awaiting Signature'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_6_date,
       MIN(CASE WHEN stage_name = '7-Closing'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_7_date,
       MIN(CASE WHEN stage_name = '8-Closed Lost'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_lost_date,
       MIN(CASE WHEN stage_name = 'Closed Won'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_8_won_date,
       MIN(CASE WHEN stage_name = '9-Unqualified'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_9_date,
       MIN(CASE WHEN stage_name = '10-Duplicate'
          THEN min_stage_date
       ELSE NULL END)                  AS min_stage_10_date,
       MIN(CASE WHEN stage_name IN ('8-Closed Lost','Closed Won','10-Duplicate','9-Unqualified')
          THEN min_stage_date
       ELSE NULL END)                  AS min_closed_stage_date,
       MIN(CASE WHEN stage_name IN ('8-Closed Lost','10-Duplicate','9-Unqualified')
          THEN min_stage_date
       ELSE NULL END)                  AS min_closed_lost_unqualified_duplicate_date

  FROM combined
  GROUP BY 1

), pre_final AS (

SELECT
        base.opportunity_id,
        opty.stage_name,
        opty.close_date,
        opty.created_date,
        opty.cycle_time_in_days,

        -- adjusted dates for throughput analysis
        -- missing stage dates are completed using the next available stage date, up to a closed date
        -- do not populate date of stages that are not reach yet
        IFF(stage_name_rank >= 0, coalesce(base.min_stage_0_date,opty.created_date),Null) as pre_stage_0_date,
        IFF(stage_name_rank >= 1, coalesce(base.min_stage_1_date,base.min_stage_2_date,base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date),Null)  AS pre_stage_1_date,
        IFF(stage_name_rank >= 2, coalesce(base.min_stage_2_date,base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date),Null)                     AS pre_stage_2_date,
        IFF(stage_name_rank >= 3, coalesce(base.min_stage_3_date,base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date),Null)                                        AS pre_stage_3_date,
        IFF(stage_name_rank >= 4, coalesce(base.min_stage_4_date,base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date),Null)                                                           AS pre_stage_4_date,
        IFF(stage_name_rank >= 5, coalesce(base.min_stage_5_date,base.min_stage_6_date,base.min_stage_7_date,base.min_stage_8_won_date) ,Null)                                              AS pre_stage_5_date,
        IFF(stage_name_rank >= 6, coalesce(base.min_stage_6_date,base.min_stage_7_date,min_stage_8_won_date) ,Null)                                                                                                  AS pre_stage_6_date,
        IFF(stage_name_rank >= 7, coalesce(base.min_stage_7_date,base.min_stage_8_won_date),Null)                                                                                                                    AS pre_stage_7_date,
        IFF(stage_name_rank = 8, base.min_stage_8_lost_date,Null)                      AS stage_8_lost_date,
        IFF(stage_name_rank = 9, base.min_stage_8_won_date,Null)                       AS stage_8_won_date,
        IFF(stage_name_rank = 11, base.min_stage_9_date,Null)                          AS stage_9_date,
        IFF(stage_name_rank = 10, base.min_stage_10_date,Null)                         AS stage_10_date,
        IFF(stage_name_rank IN (8,9),base.min_closed_stage_date,Null)                  AS stage_closed_date,
        IFF(stage_name_rank IN (8,10,11),base.min_closed_lost_unqualified_duplicate_date,Null) AS stage_close_lost_unqualified_duplicate_date,

        -- was stage skipped flag
        CASE
            WHEN not_skipped.not_skipped_stage_0_flag = 0
                AND pre_stage_0_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_0_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_1_flag = 0
                AND pre_stage_1_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_1_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_2_flag = 0
                AND pre_stage_2_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_2_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_3_flag = 0
                AND pre_stage_3_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_3_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_4_flag = 0
                AND pre_stage_4_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_4_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_5_flag = 0
                AND pre_stage_5_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_5_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_6_flag = 0
                AND pre_stage_6_date IS NOT NULL
                THEN 1
            ELSE 0
        END AS was_stage_6_skipped_flag,
        CASE
            WHEN not_skipped.not_skipped_stage_7_flag = 0
                AND pre_stage_7_date IS NOT  NULL
                THEN 1
            ELSE 0
        END AS was_stage_7_skipped_flag

    FROM pivoted_combined base
    INNER JOIN sfdc_opportunity_xf opty
      ON opty.opportunity_id = base.opportunity_id
    LEFT JOIN not_skipped
        ON not_skipped.opportunity_id = opty.opportunity_id


), adjusted_date AS (


    SELECT
        pre_final.opportunity_id,
        pre_final.stage_name,
        pre_final.close_date,
        pre_final.created_date,
        pre_final.cycle_time_in_days,

        -- second layer of adjustment, to avoid having dates
        -- show up, if previous date is higher.
        CASE
            WHEN  pre_final.pre_stage_0_date < created_date
                THEN  pre_final.created_date
            ELSE  pre_final.pre_stage_0_date
        END AS  stage_0_date,
        CASE
            WHEN  pre_final.pre_stage_1_date < pre_final.pre_stage_0_date
                THEN stage_0_date
            ELSE  pre_final.pre_stage_1_date
        END AS stage_1_date,
        CASE
            WHEN  pre_final.pre_stage_2_date < stage_1_date
                THEN stage_1_date
            ELSE  pre_final.pre_stage_2_date
        END AS stage_2_date,
        CASE
            WHEN  pre_final.pre_stage_3_date < stage_2_date
                THEN stage_2_date
            ELSE  pre_final.pre_stage_3_date
        END AS stage_3_date,
        CASE
            WHEN  pre_final.pre_stage_4_date < stage_3_date
                THEN stage_3_date
            ELSE  pre_final.pre_stage_4_date
        END AS stage_4_date,
        CASE
            WHEN  pre_final.pre_stage_5_date < stage_4_date
                THEN stage_4_date
            ELSE  pre_final.pre_stage_5_date
        END AS stage_5_date,
        CASE
            WHEN  pre_final.pre_stage_6_date < stage_5_date
                THEN stage_5_date
            ELSE  pre_final.pre_stage_6_date
        END AS stage_6_date,
        CASE
            WHEN  pre_final.pre_stage_7_date < stage_6_date
                THEN stage_6_date
            ELSE  pre_final.pre_stage_7_date
        END AS stage_7_date,

        pre_final.stage_8_lost_date,
        pre_final.stage_8_won_date,
        pre_final.stage_9_date,
        pre_final.stage_10_date,
        pre_final.stage_closed_date,
        pre_final.stage_close_lost_unqualified_duplicate_date,

        -- was stage skipped flag
        pre_final.was_stage_0_skipped_flag,
        pre_final.was_stage_1_skipped_flag,
        pre_final.was_stage_2_skipped_flag,
        pre_final.was_stage_3_skipped_flag,
        pre_final.was_stage_4_skipped_flag,
        pre_final.was_stage_5_skipped_flag,
        pre_final.was_stage_6_skipped_flag,
        pre_final.was_stage_7_skipped_flag

    FROM pre_final

), final AS (

SELECT
        base.*,

        -- calculate age in stage
        DATEDIFF(day, base.stage_0_date, coalesce(base.stage_1_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_0,
        DATEDIFF(day, base.stage_1_date, coalesce(base.stage_2_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_1,
        DATEDIFF(day, base.stage_2_date, coalesce(base.stage_3_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_2,
        DATEDIFF(day, base.stage_3_date, coalesce(base.stage_4_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_3,
        DATEDIFF(day, base.stage_4_date, coalesce(base.stage_5_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_4,
        DATEDIFF(day, base.stage_5_date, coalesce(base.stage_6_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_5,
        DATEDIFF(day, base.stage_6_date, coalesce(base.stage_7_date,base.stage_close_lost_unqualified_duplicate_date,CURRENT_DATE)) AS days_in_stage_6,
        DATEDIFF(day, base.stage_7_date, coalesce(base.stage_closed_date,CURRENT_DATE))                                             AS days_in_stage_7,

        -- stage date helpers
        stage_0.fiscal_quarter_name_fy      AS stage_0_fiscal_quarter_name,
        stage_0.first_day_of_fiscal_quarter AS stage_0_fiscal_quarter_date,
        stage_0.fiscal_year                 AS stage_0_fiscal_year,

        stage_1.fiscal_quarter_name_fy      AS stage_1_fiscal_quarter_name,
        stage_1.first_day_of_fiscal_quarter AS stage_1_fiscal_quarter_date,
        stage_1.fiscal_year                 AS stage_1_fiscal_year,

        stage_2.fiscal_quarter_name_fy      AS stage_2_fiscal_quarter_name,
        stage_2.first_day_of_fiscal_quarter AS stage_2_fiscal_quarter_date,
        stage_2.fiscal_year                 AS stage_2_fiscal_year,

        stage_3.fiscal_quarter_name_fy      AS stage_3_fiscal_quarter_name,
        stage_3.first_day_of_fiscal_quarter AS stage_3_fiscal_quarter_date,
        stage_3.fiscal_year                 AS stage_3_fiscal_year,

        stage_4.fiscal_quarter_name_fy      AS stage_4_fiscal_quarter_name,
        stage_4.first_day_of_fiscal_quarter AS stage_4_fiscal_quarter_date,
        stage_4.fiscal_year                 AS stage_4_fiscal_year,

        stage_5.fiscal_quarter_name_fy      AS stage_5_fiscal_quarter_name,
        stage_5.first_day_of_fiscal_quarter AS stage_5_fiscal_quarter_date,
        stage_5.fiscal_year                 AS stage_5_fiscal_year,

        stage_6.fiscal_quarter_name_fy      AS stage_6_fiscal_quarter_name,
        stage_6.first_day_of_fiscal_quarter AS stage_6_fiscal_quarter_date,
        stage_6.fiscal_year                 AS stage_6_fiscal_year,

        stage_7.fiscal_quarter_name_fy      AS stage_7_fiscal_quarter_name,
        stage_7.first_day_of_fiscal_quarter AS stage_7_fiscal_quarter_date,
        stage_7.fiscal_year                 AS stage_7_fiscal_year

FROM adjusted_date base
  LEFT JOIN  date_details stage_0
    ON stage_0.date_actual = base.stage_0_date::date
  LEFT JOIN  date_details stage_1
    ON stage_1.date_actual = base.stage_1_date::date
  LEFT JOIN  date_details stage_2
    ON stage_2.date_actual = base.stage_2_date::date
  LEFT JOIN  date_details stage_3
    ON stage_3.date_actual = base.stage_3_date::date
  LEFT JOIN  date_details stage_4
    ON stage_4.date_actual = base.stage_4_date::date
  LEFT JOIN  date_details stage_5
    ON stage_5.date_actual = base.stage_5_date::date
  LEFT JOIN  date_details stage_6
    ON stage_6.date_actual = base.stage_6_date::date
  LEFT JOIN  date_details stage_7
    ON stage_7.date_actual = base.stage_7_date::date

)

SELECT *
FROM final
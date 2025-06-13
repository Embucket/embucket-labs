WITH greenhouse_openings_source AS (
  SELECT *
  FROM {{ ref('greenhouse_openings_source') }}
),

greenhouse_opening_custom_fields_source AS (
  SELECT *
  FROM {{ ref('greenhouse_opening_custom_fields_source') }}
),

openings AS (
  SELECT
    job_opening_id,
    job_id,
    opening_id,
    hired_application_id,
    opening_status,
    job_opened_at,
    job_closed_at,
    close_reason,
    job_opening_created_at,
    job_opening_updated_at,
    target_start_date
  FROM greenhouse_openings_source
),

openings_custom AS (
  SELECT
    opening_id,
    MAX(IFF(opening_custom_field = 'type', opening_custom_field_display_value, NULL))                                      AS type,
    MAX(IFF(opening_custom_field = 'finance_id', opening_custom_field_display_value, NULL))                                AS finance_id,
    MAX(IFF(opening_custom_field = 'hiring_manager', opening_custom_field_display_value, NULL))                            AS hiring_manager,
    MAX(IFF(opening_custom_field = 'if_backfill_what_s_the_team_member_s_name', opening_custom_field_display_value, NULL)) AS backfill_name,
    MAX(IFF(opening_custom_field = 'sales_capacity_date', opening_custom_field_display_value, NULL))                       AS sales_capacity_date
  FROM greenhouse_opening_custom_fields_source
  GROUP BY 1
)

SELECT
  openings.*,
  openings_custom.type,
  openings_custom.finance_id,
  openings_custom.hiring_manager,
  openings_custom.backfill_name,
  openings_custom.sales_capacity_date
FROM openings
LEFT JOIN openings_custom ON openings.job_opening_id = openings_custom.opening_id

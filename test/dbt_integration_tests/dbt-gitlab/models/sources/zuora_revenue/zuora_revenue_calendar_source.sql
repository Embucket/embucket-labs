WITH zuora_revenue_calendar AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_calendar')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT
    
      CONCAT(id::VARCHAR,'01')      AS period_id,
      period_name::VARCHAR          AS period_name,
      period_num::VARCHAR           AS period_number,
      start_date::DATETIME          AS calendar_start_date,
      end_date::DATETIME            AS calendar_end_date,
      year_start_dt::DATETIME       AS year_start_date,
      qtr_num::VARCHAR              AS quarter_number,
      qtr_start_dt::DATETIME        AS quarter_start_date,
      period_year::VARCHAR          AS period_year,
      qtr_end_dt::DATETIME          AS quarter_end_date,
      year_end_dt::DATETIME         AS year_end_date,
      client_id::VARCHAR            AS client_id,
      crtd_by::VARCHAR              AS calendar_created_by,
      crtd_dt::DATETIME             AS calendar_created_date,
      updt_by::VARCHAR              AS calendar_updated_by,
      updt_dt::DATETIME             AS calendar_updated_date,
      incr_updt_dt::DATETIME        AS incremental_update_date

    FROM zuora_revenue_calendar

)

SELECT *
FROM renamed
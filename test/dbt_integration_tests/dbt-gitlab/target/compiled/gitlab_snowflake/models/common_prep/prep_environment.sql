WITH environment AS (


    SELECT
      1 AS dim_environment_id,
      'Gitlab.com' AS environment

    UNION

    SELECT
      2 AS dim_environment_id,
      'License DB' AS environment

    UNION

    SELECT
      3 AS dim_environment_id,
      'Customers Portal' AS environment

)

SELECT
      *,
      '@jpeguero'::VARCHAR       AS created_by,
      '@jpeguero'::VARCHAR       AS updated_by,
      '2021-09-22'::DATE        AS model_created_date,
      '2021-09-22'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM environment
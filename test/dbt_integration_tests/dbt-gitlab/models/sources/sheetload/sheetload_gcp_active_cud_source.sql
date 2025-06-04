WITH source AS (
    
    SELECT * 
    FROM {{ source('sheetload','gcp_active_cud') }}
    
), renamed AS (
    SELECT
      start_date::DATE                      AS start_date, 
      end_date::DATE                        AS end_date,
      vcpus::NUMBER                         AS vcpus,
      ram::NUMBER                           AS ram,
      commit_term::VARCHAR                  AS commit_term,
      region::VARCHAR                       AS region,
      machine_type::VARCHAR                 AS machine_type,
      is_active::VARCHAR                    AS is_active,
      hourly_commit_vcpus::NUMBER           AS hourly_commit_vcpus,
      hourly_commit_ram::NUMBER             AS hourly_commit_ram,
      total_hourly_commit::NUMBER           AS total_hourly_commit,
      daily_commit_amount_vcpus::NUMBER     AS daily_commit_amount_vcpus,
      daily_commit_amount_ram::NUMBER       AS daily_commit_amount_ram,
      daily_total_commit::NUMBER            AS daily_total_commit
    FROM source
)

SELECT *
FROM renamed
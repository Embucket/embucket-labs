
WITH source AS (
  SELECT *
  FROM {{ ref('snowflake_automatic_clustering_history_source') }}

),

rates AS (
  SELECT 
    *, 
    LEAD(contract_rate_effective_date, 1, current_date) OVER (ORDER BY contract_rate_effective_date) AS next_contract_rate_effective_date
  FROM {{ ref('snowflake_contract_rates_source') }}
),


include_rates AS (

  SELECT
    source.table_id,
    source.full_table_name,
    source.database_name,
    source.schema_name,
    source.table_name,
    source.clustering_start_at,
    source.clustering_end_at,
    source.bytes_reclustered,
    source.rows_reclustered,
    source.credits_used,
    source.credits_used * rates.contract_rate AS dollars_used
  FROM source
  LEFT JOIN rates
    ON source.clustering_start_at BETWEEN rates.contract_rate_effective_date AND rates.next_contract_rate_effective_date

)

SELECT *
FROM include_rates

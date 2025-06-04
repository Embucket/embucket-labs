WITH source AS (

  SELECT *
  FROM {{ ref('model_mart_crm_opportunity_id') }}

)

SELECT
    *,
    SYSDATE() as updated_at
FROM source
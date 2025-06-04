{{ config(
    tags=["mnpi_exception"]
) }}

WITH dim_marketing_contact AS (
  
    SELECT
      {{ hash_of_column('EMAIL_ADDRESS') }}
      {{ dbt_utils.star(from=ref('dim_marketing_contact'), except=['EMAIL_ADDRESS', 'FIRST_NAME', 'LAST_NAME', 'GITLAB_USER_NAME',
      'MOBILE_PHONE', 'CREATED_BY', 'UPDATED_BY', 'MODEL_CREATED_DATE', 'MODEL_UPDATED_DATE', 'DBT_UPDATED_AT', 'DBT_CREATED_AT']) }}
    FROM {{ ref('dim_marketing_contact') }}

)

{{ dbt_audit(
    cte_ref="dim_marketing_contact",
    created_by="@jpeguero",
    updated_by="@jpeguero",
    created_date="2022-04-06",
    updated_date="2022-04-19"
) }}

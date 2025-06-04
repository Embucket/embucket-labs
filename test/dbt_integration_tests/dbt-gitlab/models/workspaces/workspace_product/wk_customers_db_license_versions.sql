
WITH source AS (

  SELECT {{ hash_sensitive_columns('customers_db_license_versions_source') }}
  FROM {{ref('customers_db_license_versions_source')}}

), prepared AS (

  SELECT *
  FROM source
)

{{ dbt_audit(
    cte_ref="source",
    created_by="@rbacovic",
    updated_by="@rbacovic",
    created_date="2022-09-14",
    updated_date="2022-09-14"
) }}

WITH prep_issue_links AS (

    SELECT 
      dim_issue_link_id,
      -- FOREIGN KEYS
      -- joins to dim_issue_sk in dim_issue table
      dim_issue_sk_source,
      dim_issue_sk_target,

      /*
       We will maintain issue_ids from links source table for now due to data quality issues.
       We determined in some cases there are issues in this linkage model that do not exist in the dimension
       and will conform this in a future body of work.
      */
      issue_id_source,
      issue_id_target,

      created_at,
      updated_at,
      valid_from
    FROM {{ ref('prep_issue_links') }}

)

{{ dbt_audit(
    cte_ref="prep_issue_links",
    created_by="@dtownsend",
    updated_by="@michellecooper",
    created_date="2021-08-04",
    updated_date="2023-09-29"
) }}
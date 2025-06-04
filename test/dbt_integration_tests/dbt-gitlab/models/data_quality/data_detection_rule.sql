

WITH detection_rule AS (

    SELECT 
      1                                                 AS rule_id,
      'Missing instance types'                          AS rule_name,
      'Missing instance types for UUID or Namespaces'   AS rule_description,
      'Product'                                         AS type_of_data,
      50                                                AS threshold

   UNION

    SELECT 
      2                                                            AS rule_id,
      'Licenses with missing Subscriptions'                        AS rule_name,
      'License IDs that do not have an associated Subscription ID' AS rule_description,
      'Product'                                                    AS type_of_data,
      50                                                           AS threshold

    UNION

      SELECT 
        3                                                                               AS rule_id,
        'Subscription with paying Self-Managed Plans with missing Licenses'             AS rule_name,
        'Subscription Names that currently do not have an associated License ID'        AS rule_description,
        'Product'                                                                       AS type_of_data,
        50                                                                              AS threshold

    UNION

      SELECT 
        4                                                                                      AS rule_id,
        'Subscriptions with Self-Managed Plans having License Start dates in the future'       AS rule_name,
        'Subscription IDs with Self-Managed Plans having license_start_date in the future'     AS rule_description,
        'Product'                                                                              AS type_of_data,
        50                                                                                     AS threshold

    UNION

      SELECT 
        5                                                                                                       AS rule_id,
        'Subscriptions with Self-Managed Plans having License Start Date greater than License Expire date'      AS rule_name,
        'Subscriptions IDs with Self-Managed Plans having license_start_date greater than license_expire_date'  AS rule_description,
        'Product'                                                                                               AS type_of_data,
        50                                                                                                      AS threshold

    UNION

      SELECT 
        6                                                             AS rule_id,
        'Expired Licenses with Subscription End Dates in the Past'    AS rule_name,
        'Expired License IDs with Subscription End Dates in the Past' AS rule_description,
        'Product'                                                     AS type_of_data,
        50                                                            AS threshold

    UNION 

      SELECT 
        7                                                                                      AS rule_id,
        'Active/Paid SaaS Subscriptions Not Mapped to Namespaces'                              AS rule_name,
        'Currently paying SaaS Subscription IDs that do not have any associated Namespace IDs' AS rule_description,
        'Product'                                                                              AS type_of_data,
        50                                                                                     AS threshold
              
)

{{ dbt_audit(
    cte_ref="detection_rule",
    created_by="@snalamaru",
    updated_by="@jpeguero",
    created_date="2021-06-16",
    updated_date="2021-10-29"
) }}
with zuora_subscription_intermediate as (

    SELECT *
    FROM {{ ref ('prep_subscription_lineage_intermediate')}}

), zuora_subscription_lineage AS (

    SELECT *
    FROM {{ ref ('prep_subscription_lineage')}}

), zuora_subscription_parentage AS (

    SELECT *
    FROM {{ ref ('prep_subscription_lineage_parentage_finish')}}

), final AS (

    SELECT
      zuora_subscription_intermediate.subscription_id                                                                       AS dim_subscription_id,
      zuora_subscription_intermediate.subscription_name_slugify                                                             AS subscription_name_slugify,
      zuora_subscription_lineage.lineage                                                                                    AS subscription_lineage,
      COALESCE(zuora_subscription_parentage.ultimate_parent_sub,zuora_subscription_intermediate.subscription_name_slugify)  AS oldest_subscription_in_cohort,
      COALESCE(zuora_subscription_parentage.cohort_month, zuora_subscription_intermediate.subscription_month)               AS subscription_cohort_month,
      COALESCE(zuora_subscription_parentage.cohort_quarter,zuora_subscription_intermediate.subscription_quarter)            AS subscription_cohort_quarter,
      COALESCE(zuora_subscription_parentage.cohort_year, zuora_subscription_intermediate.subscription_year)                 AS subscription_cohort_year
    FROM zuora_subscription_intermediate
    LEFT JOIN zuora_subscription_lineage
      ON zuora_subscription_intermediate.subscription_name_slugify = zuora_subscription_lineage.subscription_name_slugify
    LEFT JOIN zuora_subscription_parentage
      ON zuora_subscription_intermediate.subscription_name_slugify = zuora_subscription_parentage.child_sub

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@iweeks",
    created_date="2021-02-11",
    updated_date="2021-07-29"
) }}

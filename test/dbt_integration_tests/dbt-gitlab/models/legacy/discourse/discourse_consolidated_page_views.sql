WITH final_select AS (

    SELECT *
    FROM {{ ref('consolidated_page_views') }}

)


{{ dbt_audit(
    cte_ref="final_select",
    created_by="@paul_armstrong",
    updated_by="@paul_armstrong",
    created_date="2020-12-01",
    updated_date="2020-12-01"
) }}

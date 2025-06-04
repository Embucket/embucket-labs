{{ config(
    tags=["mnpi_exception"]
) }}

WITH contact_role AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_contact_role_source')}}

), crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), final_opportunity_contacts AS (

    SELECT

      contact_role.opportunity_contact_role_id,
      crm_person.dim_crm_person_id,
      crm_person.sfdc_record_id,
      contact_role.contact_id,
      contact_role.opportunity_id                           AS dim_crm_opportunity_id,
      contact_role.created_by_id,
      contact_role.last_modified_by_id,
      contact_role.contact_role,
      contact_role.is_primary_contact,
      contact_role.created_date,
     {{ get_date_id('contact_role.created_date') }}        AS created_date_id,
      contact_role.last_modified_date,
     {{ get_date_id('contact_role.last_modified_date') }}  AS last_modified_date_id

    FROM contact_role
    LEFT JOIN crm_person
      ON contact_role.contact_id = crm_person.sfdc_record_id
)

{{ dbt_audit(
    cte_ref="final_opportunity_contacts",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-11-20",
    updated_date="2024-07-24"
) }}

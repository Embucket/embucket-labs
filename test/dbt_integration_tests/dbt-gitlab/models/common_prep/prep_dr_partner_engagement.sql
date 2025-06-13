{{ config(
    tags=["mnpi_exception"]
) }}

{{ generate_single_field_dimension(model_name="sfdc_opportunity_source",
                                   id_column="dr_partner_engagement",
                                   id_column_name="dim_dr_partner_engagement_id",
                                   dimension_column="dr_partner_engagement",
                                   dimension_column_name="dr_partner_engagement_name",
                                   where_clause="NOT is_deleted")
}}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-04-06",
    updated_date="2021-04-06"
) }}

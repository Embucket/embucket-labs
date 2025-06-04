{{ config(
    tags=["mnpi_exception"]
) }}

{{ generate_single_field_dimension(model_name="prep_sfdc_account",
                                   id_column="dim_account_industry_name_source",
                                   id_column_name="dim_industry_id",
                                   dimension_column="dim_account_industry_name_source",
                                   dimension_column_name="industry_name",
                                   where_clause=None)
}}


{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2021-02-02"
) }}

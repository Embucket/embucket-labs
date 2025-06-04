{{ config(
    tags=["mnpi_exception"]
) }}

{{ generate_single_field_dimension (
    model_name="prep_crm_opportunity",
    id_column="deal_path",
    id_column_name="dim_deal_path_id",
    dimension_column="deal_path",
    dimension_column_name="deal_path_name",
    where_clause="is_live"
) }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2021-02-26"
) }}

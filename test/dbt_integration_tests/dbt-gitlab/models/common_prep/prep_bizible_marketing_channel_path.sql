{{ generate_single_field_dimension (
    model_name="map_bizible_marketing_channel_path",
    id_column="bizible_marketing_channel_path_name_grouped",
    id_column_name="dim_bizible_marketing_channel_path_id",
    dimension_column="bizible_marketing_channel_path_name_grouped",
    dimension_column_name="bizible_marketing_channel_path_name",
) }}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-18",
    updated_date="2021-02-26"
) }}

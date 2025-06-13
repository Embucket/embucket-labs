{{ generate_single_field_dimension(model_name="sfdc_users_source",
                                   id_column="user_geo",
                                   id_column_name="dim_location_region_id",
                                   dimension_column="user_geo",
                                   dimension_column_name="location_region_name",
                                   where_clause=None)
}}

{{ dbt_audit(
    cte_ref="unioned",
    created_by="@mcooperDD",
    updated_by="@mcooperDD",
    created_date="2020-12-29",
    updated_date="2020-12-29"
) }}

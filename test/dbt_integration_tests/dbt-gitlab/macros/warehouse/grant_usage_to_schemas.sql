{%- macro grant_usage_to_schemas() -%}

    {#
        This works in conjunction with the Permifrost roles.yml file. 
        This will only run on production and mainly covers our bases so that
        new models created will be immediately available for querying to the 
        roles listed.

    #}

    {%- set non_sensitive = 'dbt_analytics' -%}
    {%- set sensitive = 'dbt_analytics_sensitive' -%}

    {%- if target.name == 'prod' -%}
        grant usage on schema prod.legacy to role {{ non_sensitive }};
        grant select on all tables in schema prod.legacy to role {{ non_sensitive }};
        grant select on all views in schema prod.legacy to role {{ non_sensitive }};

        grant usage on schema prod.common to role {{ non_sensitive }};
        grant select on all tables in schema prod.common to role {{ non_sensitive }};
        grant select on all views in schema prod.common to role {{ non_sensitive }};
        
        grant usage on schema prod.common_mapping to role {{ non_sensitive }};
        grant select on all tables in schema prod.common_mapping to role {{ non_sensitive }};
        grant select on all views in schema prod.common_mapping to role {{ non_sensitive }};

        grant usage on schema prod.common_mart to role {{ non_sensitive }};
        grant select on all tables in schema prod.common_mart to role {{ non_sensitive }};
        grant select on all views in schema prod.common_mart to role {{ non_sensitive }};

        grant usage on schema prod.common_mart_people to role {{ non_sensitive }};
        grant select on all tables in schema prod.common_mart_people to role {{ non_sensitive }};
        grant select on all views in schema prod.common_mart_people to role {{ non_sensitive }};

        grant usage on schema prep.sensitive to role {{ sensitive }};
        grant select on all tables in schema prep.sensitive to role {{ sensitive }};
        grant select on all views in schema prep.sensitive to role {{ sensitive }};

    {%- endif -%}

{%- endmacro -%} 

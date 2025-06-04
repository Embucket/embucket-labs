{%- macro union_tables(relations, column_override=none, include=[], exclude=[], source_column_name='_dbt_source_relation', where=none, filters={}) -%}
    {{ return(adapter.dispatch('union_tables', 'dbt_utils')(relations, column_override, include, exclude, source_column_name, where, filters)) }}
{% endmacro %}

{%- macro default__union_tables(relations, column_override=none, include=[], exclude=[], source_column_name='_dbt_source_relation', where=none, filters={}) -%}

    {%- if exclude and include -%}
        {{ exceptions.raise_compiler_error("Both an exclude and include list were provided to the `union` macro. Only one is allowed") }}
    {%- endif -%}

    {%- if not execute %}
        {{ return('') }}
    {%- endif -%}

    {%- set column_override = column_override if column_override is not none else {} -%}

    {%- set relation_columns = {} -%}
    {%- set column_superset = {} -%}
    {%- set all_excludes = exclude | map(attribute='lower') | list -%}
    {%- set all_includes = include | map(attribute='lower') | list -%}

    {%- for relation in relations -%}
        {%- do relation_columns.update({relation: []}) -%}
        {%- do dbt_utils._is_relation(relation, 'union_tables') -%}
        {%- do dbt_utils._is_ephemeral(relation, 'union_tables') -%}
        {%- set cols = adapter.get_columns_in_relation(relation) -%}
        {%- for col in cols -%}
            {%- if exclude and col.column | lower in all_excludes -%}
                {# Do nothing #}
            {%- elif include and col.column | lower not in all_includes -%}
                {# Do nothing #}
            {%- else -%}
                {%- do relation_columns[relation].append(col.column) -%}
                {%- if col.column in column_superset -%}
                    {%- set stored = column_superset[col.column] -%}
                    {%- if col.is_string() and stored.is_string() and col.string_size() > stored.string_size() -%}
                        {%- do column_superset.update({col.column: col}) -%}
                    {%- endif -%}
                {%- else -%}
                    {%- do column_superset.update({col.column: col}) -%}
                {%- endif -%}
            {%- endif -%}
        {%- endfor -%}
    {%- endfor -%}

    {%- set ordered_column_names = column_superset.keys() | list -%}
    {%- set dbt_command = flags.WHICH -%}

    {%- if dbt_command in ['run', 'build'] and (include | length > 0 or exclude | length > 0) and not ordered_column_names -%}
        {%- set relations_string = relations | map(attribute='name') | join(', ') -%}
        {%- set error_message = "There were no columns found to union for relations " ~ relations_string -%}
        {{ exceptions.raise_compiler_error(error_message) }}
    {%- endif -%}

    {%- for relation in relations %}

        (
            select
                {%- if source_column_name is not none %}
                cast({{ dbt.string_literal(relation.name) }} as {{ dbt.type_string() }}) as {{ source_column_name }},
                {%- endif %}

                {%- for col_name in ordered_column_names -%}
                    {%- set col = column_superset[col_name] %}
                    {%- set col_type = column_override.get(col.column, col.data_type) %}
                    {%- if col.is_numeric() %}
                        {%- set col_type = 'NUMBER' -%}
                    {%- endif -%}
                    {%- set col_name = adapter.quote(col_name) if col_name in relation_columns[relation] else 'null' %}
                    cast({{ col_name }} as {{ col_type }}) as {{ col.quoted }}{% if not loop.last %},{% endif %}
                {%- endfor %}

            from {{ relation }}

            {%- set relation_name = relation.name | lower -%}
            {%- set filter = filters.get(relation_name) -%}
            {% if where or filter %}
                where
                {% if where and filter -%}
                    ({{ where }}) and ({{ filter }})
                {% elif where -%}
                    {{ where }}
                {% elif filter -%}
                    {{ filter }}
                {% endif -%}
            {%- endif %}

        )

        {%- if not loop.last -%}
        union all
        {%- endif -%}

    {%- endfor %}

{%- endmacro -%}

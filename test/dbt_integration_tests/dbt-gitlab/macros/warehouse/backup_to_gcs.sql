{% macro backup_to_gcs(TABLE_LIST_BACKUP, INCLUDED = True) %}

    {%- call statement('backup', fetch_result=true, auto_begin=true) -%}

        {% set backups =
            {
                "RAW":
                    [
                        "SNAPSHOTS"
                    ]
            }
        %}

        {% set today = run_started_at.strftime("%Y_%m_%d") %}
        
        {{ log('Backing up for ' ~ today, info = true) }}

        {% for database, schemas in backups.items() %}
        
            {% for schema in schemas %}
        
                {{ log('Getting tables in schema ' ~ schema ~ '...', info = true) }}

                {% set tables = dbt_utils.get_relations_by_prefix(schema.upper(), '', exclude='FIVETRAN_%', database=database) %}

                {% for table in tables %}
                    {% if ((table.name in TABLE_LIST_BACKUP and INCLUDED == True) or (table.name not in TABLE_LIST_BACKUP and INCLUDED == False)) %}

                        {{ log('Backing up ' ~ table.name ~ '...', info = true) }}
                        {% set backup_table_command = get_backup_table_command(table, today) %}
                        {{ backup_table_command }}

                    {% endif %}
                {% endfor %}
        
            {% endfor %}
        
        {% endfor %}

    {%- endcall -%}

{%- endmacro -%}

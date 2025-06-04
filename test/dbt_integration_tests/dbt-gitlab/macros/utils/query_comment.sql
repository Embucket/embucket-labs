{% macro query_comment(node) %}
    {%- set comment_dict = {} -%}
    {%- do comment_dict.update(
        app='dbt',
        dbt_version=dbt_version,
        invocation_id=invocation_id,
        run_started_at=run_started_at.strftime("%Y-%m-%d %H:%M:%S"),
        is_full_refresh=flags.FULL_REFRESH,
        profile_name=target.get('profile_name'),
        target_name=target.get('target_name'),
        target_user=target.get('user'),
        runner=env_var('DBT_RUNNER', 'developer')
    ) -%}
    {%- if node is not none -%}
      {%- do comment_dict.update(
        file=node.original_file_path,
        node_id=node.unique_id,
        node_name=node.name,
        materialized=node.config.materialized,
        full_refresh=node.config.full_refresh,
        resource_type=node.resource_type,
        package_name=node.package_name,
        relation={
            "database": node.database,
            "schema": node.schema,
            "identifier": node.identifier
        }
      ) -%}
    {% else %}
      {%- do comment_dict.update(node_id='internal') -%}
    {%- endif -%}
    {% do return(tojson(comment_dict)) %}
{% endmacro %}

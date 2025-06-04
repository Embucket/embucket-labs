{% macro get_meta_columns(model_name, meta_key=none, node_type='model', project='gitlab_snowflake') %}

	{% if execute %}
    
        {% set meta_columns = [] %}

	    {% set fqname = node_type ~ '.' ~ project ~ '.' ~ model_name %}
	    {% set columns = graph.nodes[fqname]['columns']  %}

        {% for column in columns %}
            {% if meta_key is not none %}

                {% if graph.nodes[fqname]['columns'][column]['meta'][meta_key] == true %}

                    {# {% do log("Sensitive: " ~ column, info=true) %} #}

                    {% do meta_columns.append(column|upper) %}

                {% endif %}
            {% else %}
                {% do meta_columns.append(column|upper) %}
            {% endif %}
        {% endfor %}
	
        {{ return(meta_columns) }}

	{% endif %}

{% endmacro %}


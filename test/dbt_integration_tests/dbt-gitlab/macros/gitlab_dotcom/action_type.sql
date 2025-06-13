{%- macro action_type(action_type_id) -%}

  CASE  WHEN {{action_type_id}}::NUMBER = 1 THEN 'created'
        WHEN {{action_type_id}}::NUMBER = 2 THEN 'updated'
        WHEN {{action_type_id}}::NUMBER = 3 THEN 'closed'
        WHEN {{action_type_id}}::NUMBER = 4 THEN 'reopened'
        WHEN {{action_type_id}}::NUMBER = 5 THEN 'pushed'
        WHEN {{action_type_id}}::NUMBER = 6 THEN 'commented'
        WHEN {{action_type_id}}::NUMBER = 7 THEN 'merged'
        WHEN {{action_type_id}}::NUMBER = 8 THEN 'joined'
        WHEN {{action_type_id}}::NUMBER = 9 THEN 'left'
        WHEN {{action_type_id}}::NUMBER = 10 THEN 'destroyed'
        WHEN {{action_type_id}}::NUMBER = 11 THEN 'expired'
        END

{%- endmacro -%}

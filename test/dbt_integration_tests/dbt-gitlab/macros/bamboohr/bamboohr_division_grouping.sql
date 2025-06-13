{%- macro bamboohr_division_grouping(division) -%}

     CASE WHEN {{division}} IN ('Engineering', 'Meltano') THEN 'Engineering/Meltano'
           WHEN {{division}} IN ('CEO', 'People Group') THEN 'People Group/CEO'
           ELSE {{division}} END  

{%- endmacro -%}

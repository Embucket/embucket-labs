{%- macro user_role_mapping(user_role) -%}

    CASE
      WHEN {{user_role}} = 0 THEN 'Software Developer'
      WHEN {{user_role}} = 1 THEN 'Development Team Lead'
      WHEN {{user_role}} = 2 THEN 'Devops Engineer'
      WHEN {{user_role}} = 3 THEN 'Systems Administrator'
      WHEN {{user_role}} = 4 THEN 'Security Analyst'
      WHEN {{user_role}} = 5 THEN 'Data Analyst'
      WHEN {{user_role}} = 6 THEN 'Product Manager'
      WHEN {{user_role}} = 7 THEN 'Product Designer'
      WHEN {{user_role}} = 8 THEN 'Other'
      WHEN {{user_role}} = 99 THEN 'Experiment Default Value - Signup Not Completed'
      ELSE NULL
    END

{%- endmacro -%}

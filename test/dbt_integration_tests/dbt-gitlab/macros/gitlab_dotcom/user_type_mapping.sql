{%- macro user_type_mapping(user_type) -%}

    CASE
      WHEN {{user_type}} =  0 
        THEN 'Human'
      WHEN {{user_type}} =  1 
        THEN 'Support Bot'
      WHEN {{user_type}} =  2 
        THEN 'Alert Bot'
      WHEN {{user_type}} =  3 
        THEN 'Visual Review Bot'
      WHEN {{user_type}} =  4 
        THEN 'Service User'
      WHEN {{user_type}} =  5 
        THEN 'Ghost'
      WHEN {{user_type}} =  6 
        THEN 'Project Bot'
      WHEN {{user_type}} =  7 
        THEN 'Migration Bot'
      WHEN {{user_type}} =  8 
        THEN 'Security Bot'
      WHEN {{user_type}} =  9 
        THEN 'Automation Bot'
      WHEN {{user_type}} =  10 
        THEN 'Security Policy Bot'
      WHEN {{user_type}} =  11 
        THEN 'Admin Bot'
      WHEN {{user_type}} =  12 
        THEN 'Suggested Reviewers Bot'
      WHEN {{user_type}} =  13 
        THEN 'Service Account'
      WHEN {{user_type}} =  14 
        THEN 'LLM Bot'
      WHEN {{user_type}} =  15 
        THEN 'Placeholder'
      WHEN {{user_type}} =  16 
        THEN 'Duo Code Review Bot'
      WHEN {{user_type}} =  17 
        THEN 'Import User'
      ELSE NULL
    END

{%- endmacro -%}

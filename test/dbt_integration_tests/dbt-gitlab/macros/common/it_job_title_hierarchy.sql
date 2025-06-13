{%- macro it_job_title_hierarchy(job_title) -%}

  CASE
    WHEN LOWER({{pad_column(job_title)}}) LIKE ANY (
      '%head% it%', '%vp%technology%','%director%technology%', '%director%engineer%',
      '%chief%information%', '%chief%technology%', '%president%technology%', '%vp%technology%',
      '%director%development%', '% it%director%', '%director%information%', '%director% it%',
      '%chief%engineer%', '%director%quality%', '%vp%engineer%', '%head%information%',
      '%vp%information%', '%president%information%', '%president%engineer%',
      '%president%development%', '%director% it%', '%engineer%director%', '%head%engineer%',
      '%engineer%head%', '%chief%software%', '%director%procurement%', '%procurement%director%',
      '%head%procurement%', '%procurement%head%', '%chief%procurement%', '%vp%procurement%',
      '%procurement%vp%', '%president%procurement%', '%procurement%president%', '%head%devops%'
      )
      OR ARRAY_CONTAINS('cio'::VARIANT, SPLIT(LOWER({{job_title}}), ' '))
      OR ARRAY_CONTAINS('cio'::VARIANT, SPLIT(LOWER({{job_title}}), ','))
      OR ARRAY_CONTAINS('cto'::VARIANT, SPLIT(LOWER({{job_title}}), ' '))
      OR ARRAY_CONTAINS('cto'::VARIANT, SPLIT(LOWER({{job_title}}), ','))
      OR ARRAY_CONTAINS('cfo'::VARIANT, SPLIT(LOWER({{job_title}}), ' '))
      OR ARRAY_CONTAINS('cfo'::VARIANT, SPLIT(LOWER({{job_title}}), ','))
        THEN 'IT Decision Maker'

    WHEN LOWER({{pad_column(job_title)}}) LIKE ANY (
      '%manager%information%', '%manager%technology%', '%database%administrat%', '%manager%engineer%',
      '%engineer%manager%', '%information%manager%', '%technology%manager%', '%manager%development%',
      '%manager%quality%', '%manager%network%', '% it%manager%', '%manager% it%',
      '%manager%systems%', '%manager%application%', '%technical%manager%', '%manager%technical%',
      '%manager%infrastructure%', '%manager%implementation%', '%devops%manager%', '%manager%devops%',
      '%manager%software%', '%procurement%manager%', '%manager%procurement%'
      )
      AND NOT ARRAY_CONTAINS('project'::VARIANT, SPLIT(LOWER({{job_title}}), ' '))
        THEN 'IT Manager'

    WHEN LOWER({{pad_column(job_title)}}) LIKE ANY (
      '% it %', '% it,%', '%infrastructure%', '%engineer%',
      '%techno%', '%information%', '%developer%', '%database%',
      '%solutions architect%', '%system%', '%software%', '%technical lead%',
      '%programmer%', '%network administrat%', '%application%', '%procurement%',
      '%development%', '%tech%lead%'
      )
        THEN 'IT Individual Contributor'

    ELSE NULL

  END AS it_job_title_hierarchy

{%- endmacro -%}

{%- macro bamboohr_department_grouping(department) -%}

    CASE WHEN {{department}} IN ('Awareness', 'Communications', 'Community Relations', 'Owned Events')
           THEN 'Awareness, Communications, Community Relations, Owned Events'
         WHEN {{department}} IN ('Brand & Digital Design', 'Content Marketing', 'Inbound Marketing')
           THEN 'Brand & Digital Design, Content Marketing, Inbound Marketing'
         WHEN {{department}} IN ('Campaigns', 'Digital Marketing', 'Partner Marketing')
           THEN 'Campaigns, Digital Marketing, Partner Marketing'
         WHEN {{department}} IN ('Consulting Delivery', 'Customer Success', 'Education Delivery', 'Practice Management')
          THEN 'Consulting Delivery, Customer Success, Education Delivery, Practice Management'
         WHEN {{department}} IN ('Field Marketing', 'Marketing Ops')
          THEN 'Field Marketing, Marketing Ops'
         WHEN {{department}} IN ('People Success', 'CEO')
          THEN 'People Success, CEO'
         WHEN {{department}} IN ('Product Management', 'Product Strategy')
           THEN 'Product Management, Product Strategy'
         WHEN {{department}} in ('Field Ops - Child','Field Operations')
             THEN 'Field Ops - Child, Field Operations'
         ELSE {{department}} 
         END 

{%- endmacro -%}

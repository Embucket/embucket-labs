{% docs mart_team_member_directory %}

We are currently working on creating a table that will replace both the employee_directory_analysis and intermediate tables from the legacy schema. This is our first iteration towards achieving this goal.

Our primary focus for the upcoming iterations is to ensure that this table contains more historical data from BambooHR. Currently, it includes mostly Workday data. As a result, there are several fields with NULL values. We will be working towards decreasing the number of NULL values in these  fields in upcoming iterations. We have masked sensitive data so that only the team members with `analyst_people` role in snowflake have access. 

The grain of this table is one row per employee per valid_from/valid_to combination.

{% enddocs %}

{% docs mart_team_member_absence %}

We are currently working on creating a table that will replace both the wk_pto and intermediate tables from the legacy schema for absence models. This is our first iteration towards achieving this goal.

Our primary focus for the upcoming iterations is to ensure that this table contains more historical data from Time Off by Deel. We have masked sensitive data so that only the team members with `analyst_people` role in snowflake have access. 

The grain of this table is one row per pto_uuid per absence_date per dim_team_member_sk combination. 

{% enddocs %}


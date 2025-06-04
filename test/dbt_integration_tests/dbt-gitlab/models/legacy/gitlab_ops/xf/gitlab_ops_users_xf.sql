-- This data model code comes from https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/models/staging/gitlab_dotcom/xf/gitlab_dotcom_users_xf.sql, except we're removed references to other tables that do not exist in the data warehouse 

SELECT
    {{ dbt_utils.star(from=ref('gitlab_ops_users')) }},
    created_at                                                                    AS user_created_at,
    updated_at                                                                    AS user_updated_at, 
    TIMESTAMPDIFF(DAYS, user_created_at, last_activity_on)                        AS days_active,
    TIMESTAMPDIFF(DAYS, user_created_at, CURRENT_TIMESTAMP(2))                    AS account_age,
    CASE
      WHEN account_age <= 1 THEN '1 - 1 day or less'
      WHEN account_age <= 7 THEN '2 - 2 to 7 days'
      WHEN account_age <= 14 THEN '3 - 8 to 14 days'
      WHEN account_age <= 30 THEN '4 - 15 to 30 days'
      WHEN account_age <= 60 THEN '5 - 31 to 60 days'
      WHEN account_age > 60 THEN '6 - Over 60 days'
    END                                                                           AS account_age_cohort
FROM {{ ref('gitlab_ops_users') }}

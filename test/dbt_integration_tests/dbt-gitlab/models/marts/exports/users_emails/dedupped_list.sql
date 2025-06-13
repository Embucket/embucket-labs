WITH unioned_list as (
    SELECT
        saas_free_users.*,
        'SaaS Free User' AS bucket
    FROM static.sensitive.free_gitlab_com__20200617 as saas_free_users

    UNION

    SELECT
        saas_paid_users.*,
        'SaaS Paid User' AS bucket
    FROM static.sensitive.paid_gitlab_com__20200617 as saas_paid_users
  
    LEFT JOIN static.sensitive.ree_gitlab_com__20200617 as saas_free_users
        ON saas_free_users.notification_email = saas_paid_users.notification_email
        AND saas_free_users.full_name = saas_paid_users.full_name
  
    WHERE saas_free_users.notification_email IS NULL

    UNION

    SELECT
        self_managed_paid_users.*,
        'Self-Managed' AS bucket
    FROM static.sensitive.self_managed_gitlab_com__20200617 as self_managed_paid_users
  
    LEFT JOIN static.sensitive.paid_gitlab_com__20200617 as saas_paid_users
        ON saas_paid_users.notification_email = self_managed_paid_users.notification_email
        AND saas_paid_users.full_name         = self_managed_paid_users.full_name
  
    LEFT JOIN static.sensitive.free_gitlab_com__20200617 as saas_free_users
        ON saas_free_users.notification_email = self_managed_paid_users.notification_email
        AND saas_free_users.full_name         = self_managed_paid_users.full_name
  
    WHERE saas_free_users.notification_email IS NULL
        AND saas_paid_users.notification_email IS NULL

), unioned_list_no_dup_state AS (
    
   SELECT  DISTINCT
      user_id::NUMBER AS user_id,
      full_name,
      first_name,
      last_name,
      notification_email,
      plan_title,
      state,
      bucket
   FROM unioned_list
   QUALIFY ROW_NUMBER() OVER(PARTITION BY full_name, notification_email, plan_title ORDER BY state) = 1 -- Given a combination of email and email, if there are multiple states, only pick where active, if not inactive, if not it's only blocked

)

SELECT *
FROM unioned_list_no_dup_state

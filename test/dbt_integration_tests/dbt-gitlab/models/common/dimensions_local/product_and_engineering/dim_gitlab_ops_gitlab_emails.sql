{% set column_name = 'email_handle' %}


WITH gitlab_ops_users_xf AS (

    SELECT *
    FROM {{ref('gitlab_ops_users_xf')}} 

), intermediate AS (

    SELECT *,
       SPLIT_PART(notification_email,'@', 0)                    AS email_handle, 
      {{include_gitlab_email(column_name)}}                     AS include_email_flg
    FROM gitlab_ops_users_xf
    WHERE LENGTH(email_handle) > 1 -- removes records with just one number  
      AND notification_email ILIKE '%gitlab.com'
      AND include_email_flg = 'Include'

), final AS (

    SELECT 
      user_id, 
      user_name                                                AS gitlab_ops_user_name,
      notification_email, 
      email_handle, 
      COUNT(notification_email) OVER (PARTITION BY user_id)    AS number_of_emails 
    FROM intermediate
    GROUP BY 1,2,3,4

)

SELECT * 
FROM final 

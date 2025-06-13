{{ config(
    tags=["mnpi_exception"]
) }}

WITH namespaces AS (

  SELECT 
    creator_id,
    namespace_id
  FROM {{ref('gitlab_dotcom_namespaces_xf')}}

), user_preferences AS (

  SELECT 
    user_id,
    setup_for_company
  FROM {{ref('gitlab_dotcom_user_preferences')}}

), memberships AS (

  SELECT 
    user_id,
    ultimate_parent_id,
    is_billable
  FROM {{ref('gitlab_dotcom_memberships')}}

), users_xf AS (

  SELECT 
    user_id,
    first_name,
    last_name,
    users_name,
    notification_email
  FROM {{ref('gitlab_dotcom_users_xf')}}

), dim_marketing_contact AS (

  SELECT 
    gitlab_dotcom_user_id,
    email_address,
    dim_crm_account_id
  FROM {{ref('dim_marketing_contact')}}

), dim_crm_account AS (

  SELECT 
    crm_account_name,
    dim_crm_account_id,
    parent_crm_account_name
  FROM {{ref('dim_crm_account')}}
      
), is_user_in_company_namespace AS (

  SELECT DISTINCT 
    memberships.user_id
  FROM  namespaces 
  INNER JOIN user_preferences 
    ON user_preferences.user_id = namespaces.creator_id 
      AND user_preferences.setup_for_company = TRUE
  INNER JOIN memberships
    ON memberships.ultimate_parent_id = namespaces.namespace_id 
      AND memberships.is_billable = 'TRUE'
  
),  users AS ( 

  SELECT 
    users_xf.user_id                                                                                 AS row_integer,
    users_xf.first_name,
    users_xf.last_name, 
    users_xf.users_name,
    COALESCE(users_xf.notification_email, dim_marketing_contact.email_address)                       AS email_id,
    setup_for_company                                                                                AS internal_value1,
    CASE 
      WHEN is_user_in_company_namespace.user_id IS NOT NULL 
      THEN 1 
      ELSE 0 
    END                                                                                              AS internal_value2,
    dim_crm_account.crm_account_name                                                                 AS company_name, 
    dim_crm_account.parent_crm_account_name                                                          AS parent_company_name,
    CASE 
      WHEN email_id IS NULL 
      THEN 'missing' 
      WHEN RLIKE(SUBSTRING(email_id ,CHARINDEX('@', email_id ) +1, LEN(email_id ) - CHARINDEX('@', email_id )),'(yahoo)|(gmail)|(hotmail)|(rediff)|(outlook)|(verizon\\.net)|(live\\.)|(sbcglobal\\.net)|(laposte)|(pm\\.me)|(inbox)|(yandex)|(fastmail)|(protonmail)|(email\\.)|(att\\.net)|(posteo)|(rocketmail)|(bk\\.ru)') OR SUBSTRING(email_id ,CHARINDEX('@',email_id ) +1, LEN(email_id ) - CHARINDEX('@', email_id )) IN ('gmail.com','qq.com', 'hotmail.com','', 'yahoo.com','outlook.com','163.com','mail.ru','googlemail.com','yandex.ru', 'protonmail.com',  'icloud.com',  't-mobile.com','example.com',  'live.com', '126.com','me.com',  'gmx.de', 'hotmail.fr', 'web.de',  'google.com',  'yahoo.fr','naver.com', 'foxmail.com', 'aol.com', 'msn.com',  'hotmail.co.uk',   'ya.ru', 'wp.pl',   'gmx.net', 'live.fr','ymail.com',   'orange.fr',  'yahoo.co.uk',    'ancestry.com','free.fr', 'comcast.net', 'hotmail.de', 'mail.com', 'ukr.net',   'yahoo.co.jp',   'mac.com',  'yahoo.co.in',   'gitlab.com', 'yahoo.com.br','gitlab.localhost')  
      THEN 'personal_email' 
      ELSE 'business email' 
    END                                                                                              AS email_type 
  FROM users_xf
  LEFT JOIN user_preferences
    ON user_preferences.user_id = users_xf.user_id 
  LEFT JOIN dim_marketing_contact
   ON dim_marketing_contact.gitlab_dotcom_user_id = users_xf.user_id  
  LEFT JOIN dim_crm_account
   ON dim_crm_account.dim_crm_account_id = dim_marketing_contact.dim_crm_account_id
  LEFT JOIN is_user_in_company_namespace
   ON is_user_in_company_namespace.user_id = users_xf.user_id 


)
---- pulls all business email users or personal email users who have set up for company = True or belongs to a namespace where set up for company is true. 

SELECT *
FROM users
WHERE email_type = 'business email' OR (email_type  = 'personal_email' AND (internal_value1 = TRUE OR internal_value2 = 1)) 



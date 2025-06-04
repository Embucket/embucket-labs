WITH users AS (

    SELECT *
    FROM {{ source('gitlab_dotcom', 'users') }}
    
), memberships AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_memberships') }}
  
), plans AS (
  
    SELECT *
    FROM {{ ref('gitlab_dotcom_plans') }}

), zuora_subscription_product_category AS (

  -- Return all the subscription information 
  SELECT DISTINCT
    mrr_month,
    account_id,
    account_number,
    crm_id,
    subscription_id,
    product_category,
    delivery
  FROM {{ ref('zuora_monthly_recurring_revenue') }}

), zuora_contacts_information AS (

  -- Get the Zuora Contact information to check which columns are good 
  SELECT DISTINCT
    contact_id,
    account_id,
    first_name,
    last_name,
    work_email,
    personal_email  
  FROM {{ ref('zuora_contact') }} 

), salesforce_contacts_information AS (

  -- Get the Salesforce Contact information to check which columns are good 
  SELECT DISTINCT
      account_id,
      contact_id                                                                                                                                                AS user_id, 
      contact_name                                                                                                                                              AS full_name,
      SPLIT_PART(TRIM(contact_name), ' ', 1)                                                                                                                    AS first_name,
      ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(TRIM(contact_name), ' '), 1, 10), ' ')                                                                                  AS last_name,
      contact_email                                                                                                                                             AS email, 
      IFF(((inactive_contact = TRUE) OR (has_opted_out_email = TRUE) OR (invalid_email_address = TRUE) OR (email_is_bounced = TRUE)), 'Inactive', 'Active')     AS state 
  FROM {{ ref('sfdc_contact_source') }}

), all_gitlab_user_information AS (

  SELECT
        id                                                               AS user_id,
        TRIM(name)                                                       AS full_name,
        SPLIT_PART(TRIM(name), ' ', 1)                                   AS first_name,
        ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(TRIM(name), ' '), 1, 10), ' ') AS last_name,
        username,
        notification_email, 
        state 
    FROM users
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

), saas_paid_users AS (
  
    SELECT DISTINCT
        all_gitlab_user_information.user_id,
        all_gitlab_user_information.full_name,
        all_gitlab_user_information.first_name,
        all_gitlab_user_information.last_name,
        all_gitlab_user_information.notification_email,
        plans.plan_title,
        all_gitlab_user_information.state
    FROM all_gitlab_user_information
    LEFT JOIN memberships ON all_gitlab_user_information.user_id = memberships.user_id
    LEFT JOIN plans ON memberships.ultimate_parent_plan_id::VARCHAR = plans.plan_id::VARCHAR
    WHERE memberships.ultimate_parent_plan_id::VARCHAR IN ('2', '3', '4')
  
), zuora_subscription_product_category_saas_only AS (

  -- Filter to Self-Managed subscriptions Only 
  SELECT *
  FROM zuora_subscription_product_category
  WHERE delivery = 'SaaS'

), zuora_subscription_product_category_saas_only_current_month AS (

  SELECT *
  FROM zuora_subscription_product_category_saas_only
  WHERE mrr_month = DATE_TRUNC('month', CURRENT_DATE)

), zuora_subscription_product_category_saas_only_contacts AS (

  -- Get contact information for self-managed subscriptions 
  SELECT DISTINCT 
    --contact_id                                                                 AS user_id, 
    subscription.SUBSCRIPTION_ID,
    subscriptioN.ACCOUNT_ID,
    first_name || ' ' || last_name                                             AS full_name, 
    first_name, 
    last_name, 
    work_email                                                                 AS email, 
    subscription.product_category                                                                                              AS plan_title, 
    IFF(subscription.account_id IN
        (SELECT account_id
         FROM zuora_subscription_product_category_saas_only_current_month),
        'active', 'inactive')                                                  AS state, 
    'Zuora Only'                                                               AS source,
    NULL                                                                       AS SFDC
  FROM zuora_contacts_information AS contacts
  INNER JOIN zuora_subscription_product_category_saas_only AS subscription 
    ON contacts.account_id = subscription.account_id

), zuora_salesforce_subscription_product_category_saas_only_contacts AS (

  -- Get contact information for self-managed subscriptions 
  SELECT DISTINCT 
    --contacts.user_id, 
    subscription.SUBSCRIPTION_ID,
    subscriptioN.ACCOUNT_ID,
    contacts.full_name, 
    contacts.first_name, 
    contacts.last_name,
    contacts.email,
    subscription.product_category                 AS plan_title, 

    IFF(contacts.state = 'inactive' OR
        subscription.account_id NOT IN
        (SELECT account_id
         FROM zuora_subscription_product_category_saas_only_current_month),
        'inactive', 'active')                     AS state,
    'Zuora to Salesforce'                         AS source,
  contacts.state AS SFDC 
  
  FROM salesforce_contacts_information contacts
  INNER JOIN zuora_subscription_product_category_saas_only subscription 
    ON contacts.account_id = subscription.crm_id

), unioned_data_set AS (

  SELECT *
  FROM zuora_subscription_product_category_saas_only_contacts

  UNION ALL 

  SELECT *
  FROM zuora_salesforce_subscription_product_category_saas_only_contacts

), zuora_sfdc_contacts AS (
    SELECT DISTINCT
      NULL      AS user_id,
      full_name, 
      first_name, 
      last_name, 
      email     AS notification_email, 
      plan_title, 
      state    
    FROM unioned_data_set
    QUALIFY ROW_NUMBER() OVER(PARTITION BY full_name, email, plan_title ORDER BY state ASC) = 1  -- If user combination plan is active in one of the systems and inactive in another one, only consider where active

  -- Stops getting contact information from zuora and sfdc
 
), zuora_sfdc_contacts_no_gitlab_contacts AS (
  
    SELECT zuora_sfdc_contacts.*
    FROM zuora_sfdc_contacts
    LEFT JOIN saas_paid_users ON saas_paid_users.notification_email = zuora_sfdc_contacts.notification_email
       AND saas_paid_users.full_name = zuora_sfdc_contacts.full_name
    WHERE saas_paid_users.notification_email IS NULL

), zuora_sfdc_gitlab_users AS (
    
    SELECT *
    FROM zuora_sfdc_contacts_no_gitlab_contacts

    UNION

    SELECT *
    FROM saas_paid_users
)

SELECT DISTINCT *
FROM zuora_sfdc_gitlab_users

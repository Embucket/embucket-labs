WITH zuora_subscription_product_category AS (

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

), zuora_subscription_product_category_self_managed_only AS (

  -- Filter to Self-Managed subscriptions Only 
  SELECT *
  FROM zuora_subscription_product_category
  WHERE delivery = 'Self-Managed'

), zuora_subscription_product_category_self_managed_only_current_month AS (

  SELECT *
  FROM zuora_subscription_product_category_self_managed_only
  WHERE mrr_month = DATE_TRUNC('month', CURRENT_DATE)

), zuora_subscription_product_category_self_managed_only_contacts AS (

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
         FROM zuora_subscription_product_category_self_managed_only_current_month),
        'active', 'inactive')                                                  AS state, 
    'Zuora Only'                                                               AS source
  FROM zuora_contacts_information contacts
  INNER JOIN zuora_subscription_product_category_self_managed_only subscription 
    ON contacts.account_id = subscription.account_id

), zuora_salesforce_subscription_product_category_self_managed_only_contacts AS (

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
         FROM zuora_subscription_product_category_self_managed_only_current_month),
        'inactive', 'active')                     AS state,
    'Zuora to Salesforce'                         AS source
  
  FROM salesforce_contacts_information contacts
  INNER JOIN zuora_subscription_product_category_self_managed_only subscription 
    ON contacts.account_id = subscription.crm_id

), unioned_data_set AS (

  SELECT *
  FROM zuora_subscription_product_category_self_managed_only_contacts

  UNION ALL 

  SELECT *
  FROM zuora_salesforce_subscription_product_category_self_managed_only_contacts

), final AS (
    
  SELECT DISTINCT
    NULL      AS user_id,
    full_name, 
    first_name, 
    last_name, 
    email     AS notification_email, 
    plan_title, 
    state
  FROM unioned_data_set
  QUALIFY ROW_NUMBER() OVER(PARTITION BY full_name, email, plan_title ORDER BY state ASC) = 1   -- If user combination plan is active in one of the systems and inactive in another one, only consider where active

)

SELECT *
FROM final

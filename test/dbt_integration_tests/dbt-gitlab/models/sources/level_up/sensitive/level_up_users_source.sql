{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

{{ level_up_intermediate('users') }}

parsed AS (
  SELECT
    value['allocatedLearningPaths']::VARIANT AS allocated_learning_paths,
    value['allocatedLicenses']::VARIANT      AS allocated_licenses,
    value['asset']::VARCHAR                  AS asset,
    value['balance']::VARCHAR                AS balance,
    value['clientId']::VARCHAR               AS client_id,
    value['country']::VARCHAR                AS country,
    value['createdAt']::TIMESTAMP            AS created_at,
    value['customFields']::VARIANT           AS custom_fields,
    value['disabled']::BOOLEAN               AS is_disabled,
    value['email']::VARCHAR                  AS username,
    value['externalCustomerId']::VARCHAR     AS external_customer_id,
    value['firstName']::VARCHAR              AS first_name,
    value['id']::VARCHAR                     AS user_id,
    value['language']::VARCHAR               AS language,
    value['lastActiveAt']::TIMESTAMP         AS last_active_at,
    value['lastName']::VARCHAR               AS last_name,
    value['learnerUserId']::VARCHAR          AS learner_user_id,
    value['managerUserId']::VARCHAR          AS manager_user_id,
    value['purchasedBundles']::VARCHAR       AS purchased_bundles,
    value['purchasedCourses']::VARIANT       AS purchased_courses,
    value['ref1']::VARCHAR                   AS ref1_user_type,
    value['ref10']::VARCHAR                  AS ref10_user_dept,
    value['ref2']::VARCHAR                   AS ref2_user_job,
    value['ref4']::VARCHAR                   AS ref4_user_company,
    value['ref5']::VARCHAR                   AS ref5,
    value['ref6']::VARCHAR                   AS ref6_user_role_type,
    value['ref7']::VARCHAR                   AS ref7_user_continent,
    value['ref8']::VARCHAR                   AS ref8_user_country,
    value['ref9']::VARCHAR                   AS ref9_user_sub_dept,
    value['roleKey']::VARCHAR                AS role_key,
    value['sfAccountId']::VARCHAR            AS sf_account_id,
    value['sfContactId']::VARCHAR            AS sf_contact_id,
    value['shippingName']::VARCHAR           AS shipping_name,
    value['state']::VARCHAR                  AS state,
    value['stripeCustomerId']::VARCHAR       AS stripe_customer_id,
    value['waitlistedCourses']::VARCHAR      AS waitlisted_courses,
    value['zipCode']::VARCHAR                AS zip_code,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed

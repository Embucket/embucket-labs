{% snapshot dim_user_snapshot %}
-- Using dbt check_cols as we want only new rows when any of these columns change.
--   Cant just use updated_at because it is sometimes less than and sometimes greater than last_activity_date
    {{
        config(
          unique_key='dim_user_sk',
          strategy='check',
          check_cols=[
                      'dim_user_sk',
                      'user_id',
                      'dim_user_id',
                      'remember_created_at',
                      'sign_in_count',
                      'current_sign_in_at',
                      'last_sign_in_at',
                      'created_at',
                      'updated_at',
                      'is_admin',
                      'is_blocked_user',
                      'notification_email_domain',
                      'notification_email_domain_classification',
                      'email_domain',
                      'email_domain_classification',
                      'is_valuable_signup',
                      'public_email_domain',
                      'public_email_domain_classification',
                      'commit_email_domain',
                      'commit_email_domain_classification',
                      'identity_provider',
                      'role',
                      'last_activity_date',
                      'last_sign_in_date',
                      'setup_for_company',
                      'jobs_to_be_done',
                      'for_business_use',
                      'employee_count',
                      'country',
                      'state'
                     ],
          invalidate_hard_deletes=True
         )
    }}
    
    SELECT
    {{
          dbt_utils.star(
            from=ref('dim_user'),
            except=['DBT_UPDATED_AT',
                    'CREATED_BY',
                    'UPDATED_BY',
                    'CREATED_DATE',
                    'UPDATED_DATE',
                    'MODEL_CREATED_DATE',
                    'MODEL_UPDATED_DATE'
                   ]
            )
    }}
    FROM {{ ref('dim_user') }}

{% endsnapshot %}

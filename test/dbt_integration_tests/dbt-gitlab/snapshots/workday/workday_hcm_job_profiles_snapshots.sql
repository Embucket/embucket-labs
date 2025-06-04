{% snapshot workday_hcm_job_profiles_snapshots %}

    {{
        config(
          unique_key='job_workday_id',
          strategy='check',
          check_cols=[
            'job_code',
            'job_profile',
            'is_inactive',
            'job_family',
            'management_level',
            'job_level',
            ]
        )
    }}
    
    SELECT  job_workday_id,
            job_code,
            job_profile,
            is_inactive,
            job_family,
            management_level,
            job_level,
            report_effective_date
    FROM {{ ref('workday_hcm_job_profiles') }}

{% endsnapshot %}
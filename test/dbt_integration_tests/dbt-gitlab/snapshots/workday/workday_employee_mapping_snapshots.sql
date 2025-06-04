{% snapshot workday_employee_mapping_snapshots %}

    {{
        config(
          unique_key='employee_id',
          strategy='timestamp',
          updated_at='_fivetran_synced'
        )
    }}
    
    SELECT * 
    FROM {{ source('workday','employee_mapping') }}
    /* 
    greenhouse_candidate_id can erroneously and temporally contain text values,
    such as P-130030639002 causing the snapshot to fail on merging due to data type conflicts
    */
    WHERE TRY_TO_NUMBER(greenhouse_candidate_id) IS NOT NULL
    
{% endsnapshot %}
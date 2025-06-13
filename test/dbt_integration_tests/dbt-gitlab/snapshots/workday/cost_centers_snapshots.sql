{% snapshot cost_centers_snapshots %}

    {{
        config(
          unique_key='dept_workday_id',
          strategy='check',
          check_cols=[
            'department_name',
            'dept_inactive',
            'division_workday_id',
            'division',
            'cost_center_workday_id',
            'cost_center']
        )
    }}

    SELECT 
      report_effective_date,
      dept_workday_id,
      department_name,
      dept_inactive,
      division_workday_id,
      division,
      cost_center_workday_id,
      cost_center,
      _fivetran_deleted
    FROM {{ source('workday','cost_centers') }}
    WHERE NOT _fivetran_deleted

{% endsnapshot %}


{% snapshot workday_supervisory_org_snapshots %}

    {{
        config(
          unique_key='team_id',
          strategy='check',
          check_cols=[
            'team_hierarchy_level',
            'team_members_count',
            'team_manager_inherited',
            'team_inactivated',
            'team_manager_name',
            'team_name',
            'team_manager_name_id',
            '_fivetran_deleted',
            'team_superior_team_id',
            'team_inactivated_date']
        )
    }}
    
    SELECT
      *
    FROM {{ source('workday','supervisory_organization') }}
    
{% endsnapshot %}

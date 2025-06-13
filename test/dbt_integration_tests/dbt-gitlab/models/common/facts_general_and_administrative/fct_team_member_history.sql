WITH prep_team_member_history AS (
  SELECT *
  FROM {{ ref('prep_team_member_history') }}
),

final AS (

  SELECT *
  FROM prep_team_member_history

)

{{ dbt_audit(
    cte_ref='final',
    created_by='@rakhireddy',
    updated_by='@rakhireddy',
    created_date='2024-08-14',
    updated_date='2024-09-05',
) }}

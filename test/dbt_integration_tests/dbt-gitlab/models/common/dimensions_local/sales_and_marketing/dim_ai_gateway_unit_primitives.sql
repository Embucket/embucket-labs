{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'unit_primitive_pk')}}"
}) }}

SELECT 
  {{ dbt_utils.generate_surrogate_key(['unit_primitive_name']) }} AS unit_primitive_pk,
  unit_primitive_name,
  unit_primitive_description,
  feature_category,
  engineering_group,
  backend_services,
  available_to_license_types,
  available_to_add_ons,
  cut_off_date,
  documentation_url,
  introduced_by_url,
  unit_primitive_issue_url,
  deprecated_by_url,
  deprecation_message,
  min_gitlab_version,
  min_gitlab_version_for_free_access
FROM {{ ref('cloud_connector_configuration_source') }}
WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_participant') }}

),

renamed AS (

  SELECT

    participant_id::FLOAT AS participant_id,
    version::FLOAT AS version,
    name::VARCHAR AS name,
    descr::VARCHAR AS descr,
    region::VARCHAR AS region,
    participant_type::VARCHAR AS participant_type,
    prefix::VARCHAR AS prefix,
    first_name::VARCHAR AS first_name,
    middle_name::VARCHAR AS middle_name,
    last_name::VARCHAR AS last_name,
    employee_id::VARCHAR AS employee_id,
    salary::FLOAT AS salary,
    salary_unit_type_id::FLOAT AS salary_unit_type_id,
    hire_date::VARCHAR AS hire_date,
    termination_date::VARCHAR AS termination_date,
    personal_target::FLOAT AS personal_target,
    pr_target_unit_type_id::FLOAT AS pr_target_unit_type_id,
    native_cur_unit_type_id::FLOAT AS native_cur_unit_type_id,
    is_active::VARCHAR AS is_active,
    user_id::FLOAT AS user_id,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    entity::VARCHAR AS entity,
    adp__file__number::VARCHAR AS adp_file_number,
    sa__team::VARCHAR AS sa_team,
    position__category::VARCHAR AS position_category,
    account__owner__team::VARCHAR AS account_owner_team,
    ramping::VARCHAR AS ramping,
    segment::VARCHAR AS segment,
    department::VARCHAR AS department,
    bhr_eeid::VARCHAR AS bhr_eeid,
    bhr__title::VARCHAR AS bhr_title,
    bhr__emp_id::VARCHAR AS bhr_empid,
    effective__date::VARCHAR AS effective_date,
    annualized__variable::FLOAT AS annualized_variable,
    personal__target__local::FLOAT AS personal_target_local,
    salary__local::FLOAT AS salary_local,
    annualized__variable__local::FLOAT AS annualized_variable_local,
    semi__annual__variable__usd::FLOAT AS semi_annual_variable_usd,
    start__date_in__role::VARCHAR AS start_date_in_role,
    annualized__variable__unit_type_id::FLOAT AS annualized_variable_unittypeid,
    personal__target__local__unit_type_id::FLOAT AS personal_target_local_unittypeid,
    salary__local__unit_type_id::FLOAT AS salary_local_unittypeid,
    annualized__variable__local__unit_type_id::FLOAT AS annualized_variable_local_unittypeid,
    semi__annual__variable__usd__unit_type_id::FLOAT AS semi_annual_variable_usd_unittypeid,
    payment_cur_unit_type_id::FLOAT AS payment_cur_unit_type_id,
    source_id::FLOAT AS source_id,
    emp_status_id::FLOAT AS emp_status_id,
    effective_start_date::VARCHAR AS effective_start_date,
    effective_end_date::VARCHAR AS effective_end_date,
    is_master::VARCHAR AS is_master,
    semi__annual__variable__local::FLOAT AS semi_annual_variable_local,
    quarterly__variable__usd::FLOAT AS quarterly_variable_usd,
    quarterly__variable__local::FLOAT AS quarterly_variable_local,
    monthly__variable__local::FLOAT AS monthly_variable_local,
    monthly__variable__usd::FLOAT AS monthly_variable_usd,
    semi__annual__variable__local__unit_type_id::FLOAT AS semi_annual_variable_local_unittypeid,
    quarterly__variable__usd__unit_type_id::FLOAT AS quarterly_variable_usd_unittypeid,
    quarterly__variable__local__unit_type_id::FLOAT AS quarterly_variable_local_unittypeid,
    monthly__variable__local__unit_type_id::FLOAT AS monthly_variable_local_unittypeid,
    monthly__variable__usd__unit_type_id::FLOAT AS monthly_variable_usd_unittypeid,
    business_group_id::FLOAT AS business_group_id,
    obj_bonus_target::FLOAT AS obj_bonus_target,
    obj_bonus_target_unittype_id::FLOAT AS obj_bonus_target_unittype_id,
    obj_payment_cap_percent::FLOAT AS obj_payment_cap_percent,
    is_obj_active::VARCHAR AS is_obj_active,
    prorated_salary::FLOAT AS prorated_salary,
    prorated_personal_target::FLOAT AS prorated_personal_target,
    version_reason_id::FLOAT AS version_reason_id,
    version_sub_reason_id::FLOAT AS version_sub_reason_id

  FROM source

)

SELECT *
FROM renamed

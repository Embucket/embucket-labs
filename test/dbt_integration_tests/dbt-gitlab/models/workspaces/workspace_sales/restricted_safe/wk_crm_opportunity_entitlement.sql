{{ config(materialized='table') }}

{{ simple_cte([
('group_member', 'sfdc_group_member_source'),
('groups', 'sfdc_group_source'),
('users', 'sfdc_users_source'),
('user_roles', 'sfdc_user_roles_source'),
('profiles', 'sfdc_profile_source'),
('user_territory_association', 'sfdc_user_territory_association_source'),
('opportunity_share_active','wk_prep_crm_opportunity_share_active')]) 
}},

expanded_group_members AS (
    -- Recursive CTE to expand group memberships
    SELECT 
        group_member.user_or_group_id,
        group_member.group_id
    FROM 
        group_member
    WHERE 
        NOT EXISTS (SELECT 1 FROM groups WHERE groups.group_id = group_member.user_or_group_id)
    UNION ALL
    SELECT 
        group_member.user_or_group_id,
        expanded_group_members.group_id
    FROM 
        group_member
    JOIN 
        expanded_group_members ON group_member.group_id = expanded_group_members.user_or_group_id
    WHERE 
        EXISTS (SELECT 1 FROM groups WHERE groups.group_id = group_member.user_or_group_id)
),
user_roles_hierarchies_territories_profiles AS (
    -- CTE for user roles, hierarchies, territories, and profiles
    SELECT 
        users.user_id,
        users.profile_id,
        users.user_role_id,
        user_roles.name AS role_name,
        users.manager_id,
        user_territory_association.territory_id,
        profiles.is_permissions_view_all_data AS can_view_all_data,
        profiles.is_permissions_modify_all_data AS can_modify_all_data
    FROM 
        users
    LEFT JOIN 
        user_roles ON users.user_role_id = user_roles.id
    LEFT JOIN 
         profiles ON users.profile_id = profiles.profile_id
    LEFT JOIN 
        user_territory_association ON users.user_id = user_territory_association.user_id
    WHERE 
        users.is_active = true
)
SELECT 
    users.user_id, 
    users.user_name,
    users.profile_id,
    opportunity_share_active.opportunity_id,
    -- Apply profile-based permissions to determine the effective access level
    CASE
        WHEN user_roles_hierarchies_territories_profiles.can_modify_all_data = true THEN '1'
        WHEN user_roles_hierarchies_territories_profiles.can_view_all_data = true THEN '1'
        ELSE opportunity_share_active.opportunity_access_Level
    END AS effective_opportunity_access_level,
    user_roles_hierarchies_territories_profiles.territory_id
FROM 
    users
JOIN 
    opportunity_share_active ON users.user_id = opportunity_share_active.user_or_group_id OR opportunity_share_active.user_or_group_id IN (SELECT user_or_group_id FROM expanded_group_members)
LEFT JOIN 
    user_roles_hierarchies_territories_profiles ON users.user_id = user_roles_hierarchies_territories_profiles.user_id
QUALIFY ROW_NUMBER() OVER (PARTITION BY users.user_name, opportunity_share_active.opportunity_id ORDER BY opportunity_share_active.opportunity_access_Level DESC) = 1
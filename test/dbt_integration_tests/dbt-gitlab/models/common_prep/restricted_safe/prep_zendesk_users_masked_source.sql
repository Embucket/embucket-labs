{{ simple_cte([
    ('zendesk_users_masked_source','zendesk_users_masked_source'),
    ('zendesk_organizations_source', 'zendesk_organizations_source')
]) }}

,
final AS (
SELECT 
    -- Expanded fields from zendesk_users_masked_source
    zendesk_users_masked_source.user_id AS user_id,
    zendesk_users_masked_source.organization_id AS organization_id,
    zendesk_users_masked_source.name AS name,
    zendesk_users_masked_source.email AS email,
    zendesk_users_masked_source.phone AS phone,
    zendesk_users_masked_source.is_restricted_agent AS is_restricted_agent,
    zendesk_users_masked_source.role AS role,
    zendesk_users_masked_source.is_suspended AS is_suspended,
    zendesk_users_masked_source.time_zone AS time_zone,
    zendesk_users_masked_source.user_region AS user_region,
    zendesk_users_masked_source.tags AS tags,
    
    -- Fields from zendesk_organizations_source
    zendesk_organizations_source.sfdc_account_id AS sfdc_account_id,
    zendesk_organizations_source.organization_name AS organization_name,
    -- Date Fields from zendesk_users_masked_source
    zendesk_users_masked_source.created_at AS created_at,
    zendesk_users_masked_source.updated_at AS updated_at
FROM zendesk_users_masked_source
LEFT JOIN zendesk_organizations_source 
  ON zendesk_organizations_source.organization_id=zendesk_users_masked_source.organization_id 
)

SELECT * 
FROM final
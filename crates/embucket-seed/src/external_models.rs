// Compatibility layer

#[cfg(test)]
pub type FileVolume = api_ui::volumes::models::FileVolume;
pub type VolumePayload = api_ui::volumes::models::VolumePayload;
pub type VolumeType = api_ui::volumes::models::VolumeType;
pub type AuthResponse = api_ui::auth::models::AuthResponse;
pub type LoginPayload = api_ui::auth::models::LoginPayload;
pub type QueryCreatePayload = api_ui::queries::models::QueryCreatePayload;
pub type QueryCreateResponse = api_ui::queries::models::QueryCreateResponse;
pub type DatabasePayload = api_ui::databases::models::DatabasePayload;
pub type DatabaseCreatePayload = api_ui::databases::models::DatabaseCreatePayload;
pub type DatabaseCreateResponse = api_ui::databases::models::DatabaseCreateResponse;
pub type SchemaCreatePayload = api_ui::schemas::models::SchemaCreatePayload;
pub type SchemaCreateResponse = api_ui::schemas::models::SchemaCreateResponse;
pub type VolumeCreatePayload = api_ui::volumes::models::VolumeCreatePayload;
pub type VolumeCreateResponse = api_ui::volumes::models::VolumeCreateResponse;

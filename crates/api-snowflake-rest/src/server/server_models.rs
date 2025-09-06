use crate::models::Auth;
use core_executor::utils::DataSerializationFormat;

#[derive(Clone, Default)]
pub struct Config {
    pub auth: Auth,
    pub dbt_serialization_format: DataSerializationFormat,
}

impl Config {
    pub fn new(data_format: &str) -> std::result::Result<Self, strum::ParseError> {
        Ok(Self {
            dbt_serialization_format: DataSerializationFormat::try_from(data_format)?,
            ..Self::default()
        })
    }
    #[must_use]
    pub fn with_demo_credentials(mut self, demo_user: String, demo_password: String) -> Self {
        self.auth = Auth {
            demo_user,
            demo_password,
        };
        self
    }
}

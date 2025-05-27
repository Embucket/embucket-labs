use chrono::{DateTime, Utc};
use datafusion::common::error::Result as DFResult;
use datafusion::logical_expr::sqlparser::ast::Value;
use datafusion::logical_expr::sqlparser::ast::helpers::key_value_options::{
    KeyValueOption, KeyValueOptionType,
};
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use std::any::Any;
use std::collections::HashMap;
#[derive(Default, Debug, Clone)]
pub struct SessionParams {
    pub properties: HashMap<String, SessionProperty>,
}

#[derive(Default, Debug, Clone)]
pub struct SessionProperty {
    pub session_id: Option<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub value: String,
    pub property_type: String,
    pub comment: Option<String>,
}

impl SessionProperty {
    #[must_use]
    pub fn from_key_value(option: &KeyValueOption, session_id: String) -> Self {
        let now = Utc::now();
        Self {
            session_id: Some(session_id),
            created_on: now,
            updated_on: now,
            value: option.value.clone(),
            property_type: match option.option_type {
                KeyValueOptionType::STRING | KeyValueOptionType::ENUM => "text".to_string(),
                KeyValueOptionType::BOOLEAN => "boolean".to_string(),
                KeyValueOptionType::NUMBER => "fixed".to_string(),
            },
            comment: None,
        }
    }

    #[must_use]
    pub fn from_value(option: &Value, session_id: String) -> Self {
        let now = Utc::now();
        Self {
            session_id: Some(session_id),
            created_on: now,
            updated_on: now,
            value: match option {
                Value::Number(_, _) | Value::Boolean(_) => option.to_string(),
                _ => option.clone().into_string().unwrap_or_default(),
            },
            property_type: match option {
                Value::Number(_, _) => "fixed".to_string(),
                Value::Boolean(_) => "boolean".to_string(),
                _ => "text".to_string(),
            },
            comment: None,
        }
    }

    #[must_use]
    pub fn from_str_value(value: String, session_id: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            session_id,
            created_on: now,
            updated_on: now,
            value,
            property_type: "text".to_string(),
            comment: None,
        }
    }
}

impl SessionParams {
    pub fn set_properties(&mut self, properties: HashMap<String, SessionProperty>) -> DFResult<()> {
        for (key, value) in properties {
            self.properties.insert(key, value);
        }
        Ok(())
    }

    pub fn remove_properties(
        &mut self,
        properties: HashMap<String, SessionProperty>,
    ) -> DFResult<()> {
        for (key, ..) in properties {
            self.properties.remove(&key);
        }
        Ok(())
    }
}

impl ConfigExtension for SessionParams {
    const PREFIX: &'static str = "session_params";
}

impl ExtensionOptions for SessionParams {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        self.properties.insert(
            key.to_owned(),
            SessionProperty::from_str_value(value.to_owned(), None),
        );
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        self.properties
            .iter()
            .map(|(key, prop)| ConfigEntry {
                key: format!("session_params.{key}"),
                value: Some(prop.value.clone()),
                description: "session variable",
            })
            .collect()
    }
}

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use datafusion::common::error::Result as DFResult;
use datafusion::logical_expr::sqlparser::ast::Value;
use datafusion::logical_expr::sqlparser::ast::helpers::key_value_options::{
    KeyValueOption, KeyValueOptionType,
};
use datafusion_common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion_common::{ParamValues, ScalarValue};
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct SessionParams {
    pub properties: Arc<DashMap<String, SessionProperty>>,
}

impl Default for SessionParams {
    fn default() -> Self {
        Self {
            properties: Arc::new(DashMap::new()),
        }
    }
}

impl PartialEq for SessionParams {
    fn eq(&self, other: &Self) -> bool {
        if Arc::ptr_eq(&self.properties, &other.properties) {
            return true;
        }
        // Take a stable snapshot of both maps and compare
        let to_btree = |m: &DashMap<String, SessionProperty>| {
            let mut map: BTreeMap<String, SessionProperty> = BTreeMap::new();
            for entry in m {
                map.insert(entry.key().clone(), entry.value().clone());
            }
            map
        };
        to_btree(&self.properties) == to_btree(&other.properties)
    }
}

impl Eq for SessionParams {}

impl Hash for SessionParams {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Produce a deterministic hash by iterating in key order
        let mut items: Vec<(String, SessionProperty)> = self
            .properties
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        for (k, v) in items {
            k.hash(state);
            v.hash(state);
        }
    }
}

impl From<SessionParams> for ParamValues {
    fn from(value: SessionParams) -> Self {
        let map: HashMap<String, ScalarValue> = value
            .properties
            .iter()
            .filter_map(|entry| {
                let (key, prop) = (entry.key().clone(), entry.value().clone());
                prop.to_scalar_value().map(|scalar| (key, scalar))
            })
            .collect();
        Self::Map(map)
    }
}

#[derive(Default, Debug, PartialEq, Eq, Hash, Clone)]
pub struct SessionProperty {
    pub session_id: Option<String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub value: String,
    pub property_type: String,
    pub comment: Option<String>,
    pub name: String,
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
            name: option.option_name.clone(),
        }
    }

    #[must_use]
    pub fn from_value(name: String, option: &Value, session_id: String) -> Self {
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
            name,
        }
    }

    #[must_use]
    pub fn from_scalar_value(name: String, value: &ScalarValue, session_id: String) -> Self {
        let now = Utc::now();
        let (property_type, value) = match value {
            ScalarValue::Boolean(Some(b)) => ("boolean".to_string(), b.to_string()),
            ScalarValue::Int64(Some(i)) => ("fixed".to_string(), i.to_string()),
            ScalarValue::Float64(Some(f)) => ("fixed".to_string(), f.to_string()),
            _ => ("text".to_string(), value.to_string()),
        };
        Self {
            session_id: Some(session_id),
            created_on: now,
            updated_on: now,
            value,
            property_type,
            comment: None,
            name,
        }
    }

    #[must_use]
    pub fn from_str_value(name: String, value: String, session_id: Option<String>) -> Self {
        let now = Utc::now();
        Self {
            session_id,
            created_on: now,
            updated_on: now,
            value,
            property_type: "text".to_string(),
            comment: None,
            name,
        }
    }

    #[must_use]
    pub fn to_scalar_value(&self) -> Option<ScalarValue> {
        match self.property_type.as_str() {
            "boolean" => self
                .value
                .parse::<bool>()
                .ok()
                .map(|b| ScalarValue::Boolean(Some(b))),
            "fixed" => {
                if let Ok(i) = self.value.parse::<i64>() {
                    Some(ScalarValue::Int64(Some(i)))
                } else if let Ok(f) = self.value.parse::<f64>() {
                    Some(ScalarValue::Float64(Some(f)))
                } else {
                    None
                }
            }
            "text" => Some(ScalarValue::Utf8(Some(self.value.clone()))),
            _ => None,
        }
    }
}

impl SessionParams {
    pub fn set_properties(&self, properties: HashMap<String, SessionProperty>) -> DFResult<()> {
        for (key, value) in properties {
            self.properties.insert(key, value);
        }
        Ok(())
    }

    pub fn remove_properties(&self, properties: HashMap<String, SessionProperty>) -> DFResult<()> {
        for (key, ..) in properties {
            self.properties.remove(&key);
        }
        Ok(())
    }

    #[must_use]
    pub fn get_property(&self, key: &str) -> Option<String> {
        self.properties.get(key).map(|entry| entry.value.clone())
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
            SessionProperty::from_str_value(key.to_string(), value.to_owned(), None),
        );
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        self.properties
            .iter()
            .map(|entry| ConfigEntry {
                key: format!("session_params.{}", entry.key()),
                value: Some(entry.value().value.clone()),
                description: "session variable",
            })
            .collect()
    }
}

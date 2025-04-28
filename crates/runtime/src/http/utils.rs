use chrono::Utc;
use std::collections::HashMap;

#[allow(clippy::implicit_hasher)]
pub fn update_properties_timestamps(properties: &mut HashMap<String, String>) {
    let utc_now = Utc::now();
    let utc_now_str = utc_now.to_rfc3339();
    properties.insert("created_at".to_string(), utc_now_str.clone());
    properties.insert("updated_at".to_string(), utc_now_str);
}

#[must_use]
pub fn get_default_properties() -> HashMap<String, String> {
    let mut properties = HashMap::new();
    update_properties_timestamps(&mut properties);
    properties
}

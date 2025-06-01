//! Module for parsing seed templates into structured data.
//!
use crate::seed_models::SeedTemplateRoot;

/// Parses a YAML string into a `SeedTemplateRoot` structure.
///
/// This function takes a YAML-formatted string and attempts to deserialize it
/// into a `SeedTemplateRoot` structure that represents the seed data hierarchy.
///
/// # Arguments
/// * `seed_template` - A string slice containing the YAML seed template
///
/// # Returns
/// A `Result` containing the parsed template `SeedTemplateRoot` on success, or a `serde_yaml::Error`
/// if parsing fails.
pub fn parse_seed_template(seed_template: &str) -> Result<SeedTemplateRoot, serde_yaml::Error> {
    serde_yaml::from_str::<SeedTemplateRoot>(seed_template)
}

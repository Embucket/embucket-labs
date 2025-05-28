//! Module for managing static seed data assets.
//!
//! This module provides predefined seed data templates pre-compiled into the `embucket` binary.
//! Seed generated from these templates can be used to feed running service with test data.
//! The templates vary in size and complexity.

use clap::ValueEnum;

/// Enumerates the available seed data variants.
///
/// Each variant corresponds to a different YAML template file containing
/// the seed data configuration. The variants are ordered from smallest to largest
/// in terms of the amount of data they generate.
#[derive(Copy, Clone, ValueEnum, Debug)]
pub enum SeedVariant {
    /// Minimal seed data with the smallest possible dataset.
    /// Suitable for quick tests and basic functionality verification.
    Minimal,

    /// A typical dataset representing common usage patterns.
    /// Provides a good balance between coverage and performance.
    Typical,

    /// A large dataset designed for stress testing and performance evaluation.
    /// Generates significantly more data than the other variants.
    Extreme,
}

impl SeedVariant {
    /// Returns the YAML content of the seed template for this variant.
    ///
    /// The template is loaded at compile time and contains the configuration
    /// for generating the seed data.
    ///
    /// # Returns
    /// A string slice containing the YAML template content.
    #[must_use]
    pub const fn seed_data(&self) -> &'static str {
        match self {
            Self::Minimal => include_str!("../templates/minimal_seed.yaml"),
            Self::Typical => include_str!("../templates/typical_seed.yaml"),
            Self::Extreme => include_str!("../templates/extreme_seed.yaml"),
        }
    }
}

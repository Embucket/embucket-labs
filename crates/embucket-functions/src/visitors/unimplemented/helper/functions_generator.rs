use std::collections::BTreeSet;
use std::fs;
use std::path::Path;

use crate::table::register_udtfs;
use crate::{register_udafs, register_udfs};
use core_history::store::SlateDBHistoryStore;
use datafusion::prelude::SessionContext;

/// Generates the implemented_functions.csv file by extracting all function names
/// from a fully configured DataFusion SessionContext.
pub async fn generate_implemented_functions_csv() -> Result<(), Box<dyn std::error::Error>> {
    println!("Generating implemented_functions.csv...");

    // Create a SessionContext and register all the functions like in session.rs
    let mut ctx = SessionContext::new();

    // Register custom functions
    register_udfs(&mut ctx)?;
    register_udafs(&mut ctx)?;

    // Register table functions with an in-memory history store
    let history_store = SlateDBHistoryStore::new_in_memory().await;
    register_udtfs(&ctx, history_store);

    // Register JSON functions
    datafusion_functions_json::register_all(&mut ctx)?;

    let state = ctx.state();

    let mut all_functions = BTreeSet::new();

    // Add scalar functions
    for name in state.scalar_functions().keys() {
        all_functions.insert(name.clone());
    }

    // Add aggregate functions
    for name in state.aggregate_functions().keys() {
        all_functions.insert(name.clone());
    }

    // Add window functions
    for name in state.window_functions().keys() {
        all_functions.insert(name.clone());
    }

    // Add table functions
    for name in state.table_functions().keys() {
        all_functions.insert(name.clone());
    }

    // Create the CSV content
    let mut csv_content = String::new();
    csv_content.push_str("IMPLEMENTED_FUNCTIONS\n");

    let function_count = all_functions.len();
    for function_name in all_functions {
        csv_content.push_str(&function_name);
        csv_content.push('\n');
    }

    // Write to the current helper directory
    let csv_path = Path::new("implemented_functions.csv");
    fs::write(&csv_path, csv_content)?;

    println!(
        "✅ Generated implemented_functions.csv with {} functions",
        function_count
    );
    println!("📁 File location: {}", csv_path.display());

    Ok(())
}

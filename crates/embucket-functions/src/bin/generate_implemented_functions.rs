use core_history::store::SlateDBHistoryStore;
use datafusion::prelude::SessionContext;
use embucket_functions::session_params::SessionParams;
use embucket_functions::table::register_udtfs;
use embucket_functions::{register_udafs, register_udfs};
use std::collections::BTreeSet;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// Find the project root by looking for the crates folder
fn find_project_root() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let mut current_dir = std::env::current_dir()?;

    loop {
        let crates_dir = current_dir.join("crates");
        if crates_dir.exists() && crates_dir.is_dir() {
            return Ok(current_dir);
        }

        match current_dir.parent() {
            Some(parent) => current_dir = parent.to_path_buf(),
            None => return Err("Could not find project root with crates folder".into()),
        }
    }
}

/// Generates the `implemented_functions.csv` file by extracting all function names
/// from a fully configured `DataFusion` `SessionContext`.
pub async fn generate_implemented_functions_csv() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Generating implemented_functions.csv...");

    // Find project root and construct the CSV path
    let project_root = find_project_root()?;
    let csv_path = project_root
        .join("crates")
        .join("embucket-functions")
        .join("src")
        .join("visitors")
        .join("unimplemented")
        .join("helper")
        .join("implemented_functions.csv");

    // Create a SessionContext and register all the functions like in session.rs
    let mut ctx = SessionContext::new();

    register_udfs(&mut ctx, &Arc::new(SessionParams::default()))?;
    register_udafs(&mut ctx)?;

    let history_store = SlateDBHistoryStore::new_in_memory().await;
    register_udtfs(&ctx, history_store);

    datafusion_functions_json::register_all(&mut ctx)?;

    let state = ctx.state();

    // Functions that are currently implemented as alias functions in the core-executor crate
    let exceptions_to_ignore: Vec<String> = vec!["grouping_id".to_string()];

    let all_functions: BTreeSet<_> = state
        .scalar_functions()
        .keys()
        .chain(state.aggregate_functions().keys())
        .chain(state.window_functions().keys())
        .chain(state.table_functions().keys())
        .chain(exceptions_to_ignore.iter())
        .cloned()
        .collect();

    // Create the CSV content
    let mut csv_content = String::new();
    csv_content.push_str("IMPLEMENTED_FUNCTIONS\n");

    let function_count = all_functions.len();
    for function_name in all_functions {
        csv_content.push_str(&function_name);
        csv_content.push('\n');
    }

    // Ensure the directory exists
    if let Some(parent) = csv_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Write to the helper directory
    fs::write(&csv_path, csv_content)?;

    eprintln!("✅ Generated implemented_functions.csv with {function_count} functions");
    eprintln!("📁 File location: {}", csv_path.display());

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = generate_implemented_functions_csv().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}

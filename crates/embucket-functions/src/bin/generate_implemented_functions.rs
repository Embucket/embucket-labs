use embucket_functions::visitors::unimplemented::helper::functions_generator::generate_implemented_functions_csv;
use std::env;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Change to the helper directory so the CSV is written in the right place
    let helper_dir = "src/visitors/unimplemented/helper";
    env::set_current_dir(helper_dir)?;

    // Generate the CSV file
    generate_implemented_functions_csv().await?;

    Ok(())
}

use std::env;
use std::fs::File;
use std::path::Path;
use tar::Builder;

fn create_web_assets_tarball() -> Result<(), std::io::Error> {
    let source_path = Path::new(env!("WEB_ASSETS_SOURCE_PATH"));
    if !source_path.exists() {
        eprintln!("Source path does not exist: {}", source_path.display());
    }
    let tarball_path = Path::new(env!("WEB_ASSETS_TARBALL_PATH"));
    let tar = File::create(tarball_path)?;
    let mut tar = Builder::new(tar);
    tar.append_dir_all("", source_path)?;
    tar.finish()?;
    Ok(())
}

#[allow(clippy::unwrap_used)]
fn main() {
    create_web_assets_tarball().unwrap();
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed={}", env!("WEB_ASSETS_SOURCE_PATH"));
    println!(
        "cargo::rerun-if-changed={}",
        env!("WEB_ASSETS_TARBALL_PATH")
    );
}

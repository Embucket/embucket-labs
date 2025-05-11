use std::env;
use std::fs::File;
use std::path::Path;
use std::time::Duration;
use tar::Builder;

fn create_web_assets_tarball() -> Result<(), std::io::Error> {
    let source_path = Path::new(env!("WEB_ASSETS_SOURCE_PATH"));
    if !source_path.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Source path does not exist: {}", source_path.display()),
        ));
    }
    let tarball_path = Path::new(env!("WEB_ASSETS_TARBALL_PATH"));
    let tar = File::create(tarball_path)?;
    let mut tar = Builder::new(tar);
    tar.append_dir_all("", source_path)?;
    tar.finish()?;

    Ok(())
}

#[allow(clippy::expect_used)]
#[tokio::main]
async fn main() {
    let tar_file_task = tokio::spawn(async {
        let res = create_web_assets_tarball();
        if let Err(err) = res {
            panic!("Error creating web assets tarball: {err}");
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
        eprintln!("Web assets tarball created");
    });
    let _ = tokio::try_join!(tar_file_task);

    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed={}", env!("WEB_ASSETS_SOURCE_PATH"));
    println!(
        "cargo::rerun-if-changed={}",
        env!("WEB_ASSETS_TARBALL_PATH")
    );
}

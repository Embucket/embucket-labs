// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

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

#[allow(clippy::expect_used)]
fn main() {
    create_web_assets_tarball().expect("Failed to create web assets tarball");
    println!("cargo::rerun-if-changed=build.rs");
    println!("cargo::rerun-if-changed={}", env!("WEB_ASSETS_SOURCE_PATH"));
    println!(
        "cargo::rerun-if-changed={}",
        env!("WEB_ASSETS_TARBALL_PATH")
    );
}

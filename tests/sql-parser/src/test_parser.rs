use sqlparser::dialect::SnowflakeDialect;
use sqlparser::parser::Parser;
use std::io::{self, BufRead, BufReader, Read};
use std::path::Path;
use std::{env, fs};

fn find_testlist_files(dir: &str) -> io::Result<Vec<String>> {
    let mut testlist_files = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("testlist") {
            testlist_files.push(path.display().to_string());
        }
    }
    Ok(testlist_files)
}

fn read_file_list(file_path: &str) -> io::Result<Vec<String>> {
    let file = fs::File::open(file_path)?;
    let reader = BufReader::new(file);

    let testlist_dir = Path::new(file_path)
        .parent()
        .unwrap_or_else(|| Path::new("."));

    let mut file_names = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            // Join the SQL file path with the `.testlist` directory
            let full_path = testlist_dir.join(line.trim());
            file_names.push(full_path.display().to_string());
        }
    }

    Ok(file_names)
}

fn read_sql_file(file_path: &str) -> io::Result<String> {
    let file = fs::File::open(file_path)?;
    let mut reader = BufReader::new(file);
    let mut content = String::new();
    reader.read_to_string(&mut content)?;
    Ok(content)
}

#[test]
fn validate_sql_syntax_in_files() {
    let testlist_file_env = env::var("TESTLIST_FILE").ok();
    let directory = "sql-tests"; // Current directory
    let testlist_files = if let Some(testlist_file) = testlist_file_env {
        vec![testlist_file]
    } else {
        find_testlist_files(directory).expect("Failed to find .testlist files")
    };

    if testlist_files.is_empty() {
        panic!("No .testlist files were found in the current directory.");
    }

    let dialect = SnowflakeDialect;

    let mut files_valid = true;

    for testlist_file in &testlist_files {
        println!("Processing testlist file: {}", testlist_file);
        let sql_files = read_file_list(&testlist_file).unwrap_or_else(|err| {
            panic!("Failed to read testlist file {}: {}", testlist_file, err)
        });

        if sql_files.is_empty() {
            panic!("The testlist file {} is empty.", testlist_file);
        }
        for file_path in sql_files {
            println!("Validating file: {}", file_path);

            let sql_content = read_sql_file(&file_path)
                .unwrap_or_else(|err| panic!("Failed to read the SQL file {}: {}", file_path, err));

            // Split sql queries by semicolon
            let queries: Vec<&str> = sql_content
                .split(';')
                .map(|q| q.trim())
                .filter(|q| !q.is_empty())
                .collect();

            let mut valid_file = true;
            for (i, query) in queries.iter().enumerate() {
                //println!("Validating query {} in file {}: {}", i + 1, file_path, query);

                match Parser::parse_sql(&dialect, query) {
                    Ok(_ast) => {
                        //println!("Query {} in file {} is valid.", i + 1, file_path);
                    }
                    Err(err) => {
                        println!(
                            "Query {}: {} in file {} failed to parse: {}",
                            i + 1,
                            query,
                            file_path,
                            err
                        );
                        valid_file = false;
                    }
                }
            }

            if valid_file {
                println!("File {} is valid.", file_path);
            } else {
                println!("File {} is invalid.", file_path);
                files_valid = false;
            }
        }
    }

    println!(
        "All SQL files have been validated."
    );
    assert!(files_valid);
}

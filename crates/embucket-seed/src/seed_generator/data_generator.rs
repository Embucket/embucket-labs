use datafusion::arrow::csv::WriterBuilder;
use reqwest::blocking::Client;
use reqwest::blocking::multipart;
use serde::Serialize;
use std::error::Error;

use crate::seed_models::Table;

#[derive(Serialize)]
struct Person {
    name: String,
    age: u32,
    email: String,
}

pub fn generate_data() -> Vec<String> {
    // 1. Write CSV into memory (Vec<u8>)
    let mut buffer = Vec::new();
    let mut writer = WriterBuilder::new().has_headers(true).from_writer(&mut buffer);

    let people = vec![
        Person {
            name: "Alice".into(),
            age: 30,
            email: "alice@example.com".into(),
        },
        Person {
            name: "Bob".into(),
            age: 25,
            email: "bob@example.com".into(),
        },
    ];

    for person in people {
        writer.serialize(person)?;
    }

    writer.flush()?; // important!

    // 2. Build multipart form with CSV as file part
    let part = multipart::Part::bytes(buffer)
        .file_name("people.csv")
        .mime_str("text/csv")?;

    let form = multipart::Form::new().part("file", part);

    // 3. Send POST request
    let response = Client::new()
        .post("http://localhost:8000/upload") // change to your endpoint
        .multipart(form)
        .send()?;

    println!("Status: {}", response.status());
    Ok(())

}

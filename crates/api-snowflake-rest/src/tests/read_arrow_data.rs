use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;
use base64::{Engine as _, engine::general_purpose};
use std::io::Cursor;

#[must_use]
pub fn read_record_batches_from_arrow_data(base64_data: &str) -> Vec<RecordBatch> {
    let raw_bytes = general_purpose::STANDARD
        .decode(base64_data)
        .expect("invalid base64");
    let cursor = Cursor::new(raw_bytes);
    let mut reader = StreamReader::try_new(cursor, None).expect("failed to create reader");

    let mut record_batches = Vec::new();
    while let Some(Ok(batch)) = reader.next() {
        record_batches.push(batch);
    }
    record_batches
}

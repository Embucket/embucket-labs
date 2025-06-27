use crate::{ArrowSnafu, Error, Utf8Snafu};
use base64::engine::general_purpose::STANDARD as engine_base64;
use base64::Engine;
use datafusion::arrow::ipc::writer::{IpcWriteOptions, StreamWriter};
use datafusion::arrow::ipc::MetadataVersion;
use datafusion::arrow::json::writer::JsonArray;
use datafusion::arrow::json::WriterBuilder;
use datafusion::arrow::record_batch::RecordBatch;
use snafu::ResultExt;

// https://arrow.apache.org/docs/format/Columnar.html#buffer-alignment-and-padding
// Buffer Alignment and Padding: Implementations are recommended to allocate memory
// on aligned addresses (multiple of 8- or 64-bytes) and pad (overallocate) to a
// length that is a multiple of 8 or 64 bytes. When serializing Arrow data for interprocess
// communication, these alignment and padding requirements are enforced.
// For more info see issue #115
const ARROW_IPC_ALIGNMENT: usize = 8;

pub fn records_to_arrow_string(recs: &Vec<RecordBatch>) -> Result<String, Error> {
    let mut buf = Vec::new();
    let options = IpcWriteOptions::try_new(ARROW_IPC_ALIGNMENT, false, MetadataVersion::V5)
        .context(ArrowSnafu)?;
    if !recs.is_empty() {
        let mut writer =
            StreamWriter::try_new_with_options(&mut buf, recs[0].schema_ref(), options)
                .context(ArrowSnafu)?;
        for rec in recs {
            writer.write(rec).context(ArrowSnafu)?;
        }
        writer.finish().context(ArrowSnafu)?;
    }
    Ok(engine_base64.encode(buf))
}

pub fn records_to_json_string(recs: &[RecordBatch]) -> Result<String, Error> {
    let buf = Vec::new();
    let write_builder = WriterBuilder::new().with_explicit_nulls(true);
    let mut writer = write_builder.build::<_, JsonArray>(buf);
    let record_refs: Vec<&RecordBatch> = recs.iter().collect();
    writer.write_batches(&record_refs).context(ArrowSnafu)?;
    writer.finish().context(ArrowSnafu)?;

    // Get the underlying buffer back,
    String::from_utf8(writer.into_inner()).context(Utf8Snafu)
}

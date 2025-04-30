use datafusion_common::DataFusionError;
use std::future::Future;

pub mod catalog;
pub mod iceberg_catalog;
pub mod schema;

fn block_on_with_fallback<F, R>(future: F) -> Result<R, DataFusionError>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        Ok(handle.block_on(future))
    } else {
        std::thread::spawn(move || {
            tokio::runtime::Runtime::new()
                .map(|rt| rt.block_on(future))
                .map_err(|_| {
                    DataFusionError::Execution("failed to create Tokio runtime".to_string())
                })
        })
        .join()
        .unwrap_or_else(|_| Err(DataFusionError::Execution("thread panicked".to_string())))
    }
}

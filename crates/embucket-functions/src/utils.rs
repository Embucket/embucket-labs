use datafusion_common::DataFusionError;
use tokio::runtime::Builder;

pub fn block_in_new_runtime<F, R>(future: F) -> Result<R, DataFusionError>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(move || {
        Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| DataFusionError::Execution("Failed to create Tokio runtime".to_string()))
            .map(|rt| rt.block_on(future))
    })
    .join()
    .unwrap_or_else(|_| {
        Err(DataFusionError::Execution(
            "Thread panicked while executing future".to_string(),
        ))
    })
}

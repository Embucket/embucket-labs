use crate::df_error;
use datafusion_common::DataFusionError;
use snafu::ResultExt;
use std::future::Future;
use tokio::runtime::Builder;

pub fn block_in_new_runtime<F, R>(future: F) -> Result<R, DataFusionError>
where
    F: Future<Output = R> + Send + 'static,
    R: Send + 'static,
{
    std::thread::spawn(move || {
        Ok(Builder::new_current_thread()
            .enable_all()
            .build()
            .map(|rt| rt.block_on(future))
            .context(df_error::FailedToCreateTokioRuntimeSnafu)?)
    })
    .join()
    .unwrap_or_else(|_| {
        // using .fail()? instead of .build() to do implicit into conversion
        // from our custom DataFusionExecutionError to DataFusionError
        df_error::ThreadPanickedWhileExecutingFutureSnafu.fail()?
    })
}

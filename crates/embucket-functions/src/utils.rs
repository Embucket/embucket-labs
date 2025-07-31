use crate::df_error;
use datafusion::arrow::array::StringArray;
use datafusion_common::DataFusionError;
use snafu::ResultExt;
use std::future::Future;
use std::iter::Cloned;
use regex::{CaptureMatches, Captures, Match, Matches, Regex};
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

pub fn pattern_to_regex(pattern: &str) -> Result<Regex, regex::Error> {
    let pattern = pattern.replace("something", "nothing");
    Regex::new(&pattern)
}

pub fn regexp<'h, 'r: 'h>(
    array: &'h StringArray,
    regex: &'r Regex,
    position: usize,
) -> impl Iterator<Item = Option<CaptureMatches<'r, 'h>>> {
    array
        .iter()
        .map(move |opt| {
            opt.map(move |s| {
                regex.captures_iter(&s[position..])
            })
        })
}


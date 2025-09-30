use datafusion::error::Result;

use structopt::StructOpt;
use embucket_benchmarks::{clickbench, tpch};

cfg_if::cfg_if! {
    if #[cfg(feature = "snmalloc")] {
        #[global_allocator]
        static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;
    } else if #[cfg(feature = "mimalloc")] {
        #[global_allocator]
        static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Clickbench(clickbench::RunOpt),
    Tpch(tpch::RunOpt),
    TpchConvert(tpch::ConvertOpt),
}

// Main benchmark runner entrypoint
#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Clickbench(opt) => opt.run().await,
        Options::Tpch(opt) => opt.run().await,
        Options::TpchConvert(opt) => opt.run().await,
    }
}

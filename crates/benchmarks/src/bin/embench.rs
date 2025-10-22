use datafusion::error::Result;

use embucket_benchmarks::{clickbench, tpch};
use structopt::StructOpt;

cfg_if::cfg_if! {
    if #[cfg(feature = "jemalloc")] {
        #[global_allocator]
        static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
    } else if #[cfg(feature = "mimalloc")] {
        #[global_allocator]
        static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
    }
}

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
enum Options {
    Tpch(tpch::RunOpt),
    DfTpch(tpch::RunOpt),
    Clickbench(clickbench::RunOpt),
    DfClickbench(clickbench::RunOpt),
    TpchConvert(tpch::ConvertOpt),
}

// Main benchmark runner entrypoint
#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    match Options::from_args() {
        Options::Tpch(opt) | Options::DfTpch(opt) => opt.run().await,
        Options::Clickbench(opt) | Options::DfClickbench(opt) => opt.run().await,
        Options::TpchConvert(opt) => opt.run().await,
    }
}

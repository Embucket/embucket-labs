//! tpch binary only entrypoint

extern crate embucket_benchmarks;
extern crate structopt;

use datafusion::error::Result;
use embucket_benchmarks::tpch;
use structopt::StructOpt;

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
enum BenchmarkSubCommandOpt {
    #[structopt(name = "embucket")]
    EmbucketBenchmark(tpch::RunOpt),
}

#[derive(Debug, StructOpt)]
#[structopt(name = "TPC-H", about = "TPC-H Benchmarks.")]
enum TpchOpt {
    Benchmark(BenchmarkSubCommandOpt),
    Convert(tpch::ConvertOpt),
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    match TpchOpt::from_args() {
        TpchOpt::Benchmark(BenchmarkSubCommandOpt::EmbucketBenchmark(opt)) => opt.run().await,
        TpchOpt::Convert(opt) => opt.run().await,
    }
}

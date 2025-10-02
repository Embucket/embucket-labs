use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// The number of output parquet files
    #[structopt(long = "output_files_number", default_value = "1")]
    pub output_files_number: usize,
}

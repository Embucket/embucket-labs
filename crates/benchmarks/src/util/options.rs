use structopt::StructOpt;

// hack to avoid `default_value is meaningless for bool` errors
type BoolDefaultTrue = bool;

#[derive(Debug, StructOpt, Clone)]
pub struct CommonOpt {
    /// Number of iterations of each test run
    #[structopt(short = "i", long = "iterations", default_value = "3")]
    pub iterations: usize,

    /// The number of output parquet files
    #[structopt(long = "output_files_number", default_value = "1")]
    pub output_files_number: usize,

    /// If true then hash join used, if false then sort merge join
    /// True by default.
    #[structopt(short = "j", long = "prefer_hash_join", default_value = "true")]
    pub prefer_hash_join: BoolDefaultTrue,
}

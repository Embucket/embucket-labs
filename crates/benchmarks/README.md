# Embucket Benchmarks

This crate contains benchmarks based on popular public data sets and
open source benchmark suites, to help with performance and scalability
testing of DataFusion.

# Running the benchmarks

## `bench.sh`

The easiest way to run benchmarks is the [bench.sh](bench.sh)
script. Usage instructions can be found with:

```shell
# show usage
cd ./benchmarks/
./bench.sh
```

## Generating data

You can create / download the data for these benchmarks using the [bench.sh](bench.sh) script:

Create / download all datasets

```shell
./bench.sh data
```

Create / download a specific dataset (TPCH)

```shell
./bench.sh data tpch
```
Data is placed in the `data` subdirectory.

## Running benchmarks

Run benchmark for TPC-H dataset
```shell
./bench.sh run tpch
```
or for TPC-H dataset scale 10
```shell
./bench.sh run tpch10
```

To run for specific query, for example Q21
```shell
./bench.sh run tpch10 21
```
### Running Benchmarks Manually

Assuming data is in the `data` directory, the `tpch` benchmark can be run with a command like this:

```bash
cargo run --release --bin dfbench -- tpch --iterations 3 --path ./data  --query 1
```

See the help for more details.

### Different features

You can enable `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as `--features`. For example:

```shell
cargo run --release --features "mimalloc" --bin tpch -- benchmark datafusion --iterations 3 --path ./data --query 1
```

# Writing a new benchmark

## Creating or downloading data outside of the benchmark

If you want to create or download the data with Rust as part of running the benchmark, see the next
section on adding a benchmark subcommand and add code to create or download data as part of its
`run` function.

If you want to create or download the data with shell commands, in `benchmarks/bench.sh`, define a
new function named `data_[your benchmark name]` and call that function in the `data` command case
as a subcommand case named for your benchmark. Also call the new function in the `data all` case.

## Adding the benchmark subcommand

In `benchmarks/bench.sh`, define a new function named `run_[your benchmark name]` following the
example of existing `run_*` functions. Call that function in the `run` command case as a subcommand
case named for your benchmark. subcommand for your benchmark. Also call the new function in the
`run all` case. Add documentation for your benchmark to the text in the `usage` function.

### Creating or downloading data as part of the benchmark

Use the `--path` structopt field defined on the `RunOpt` struct to know where to store or look for
the data. Generate the data using whatever Rust code you'd like, before the code that will be
measuring an operation.

### Collecting data

Your benchmark should create and use an instance of `BenchmarkRun` defined in `benchmarks/src/util/run.rs` as follows:

- Call its `start_new_case` method with a string that will appear in the "Query" column of the
  compare output.
- Use `write_iter` to record elapsed times for the behavior you're benchmarking.
- When all cases are done, call the `BenchmarkRun`'s `maybe_write_json` method, giving it the value
  of the `--output` structopt field on `RunOpt`.

## ClickBench

The ClickBench[1] benchmarks are widely cited in the industry and
focus on grouping / aggregation / filtering. This runner uses the
scripts and queries from [2].

[1]: https://github.com/ClickHouse/ClickBench
[2]: https://github.com/ClickHouse/ClickBench/tree/main/datafusion


## TPCH

Run the tpch benchmark.

This benchmarks is derived from the [TPC-H][1] version
[2.17.1]. The data and answers are generated using `tpch-gen` from
[2].

[1]: http://www.tpc.org/tpch/
[2]: https://github.com/databricks/tpch-dbgen.git,
[2.17.1]: https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v2.17.1.pdf

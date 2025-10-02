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

## Comparing performance of main and a branch

```shell
git checkout main

# Create the data
./benchmarks/bench.sh data tpch

# Gather baseline data for tpch benchmark
./benchmarks/bench.sh run tpch

# Switch to the branch named mybranch and gather data
git checkout mybranch
./benchmarks/bench.sh run tpch

# Compare results in the two branches:
./bench.sh compare main mybranch
```

This produces results like:

```shell
Comparing main and mybranch
--------------------
Benchmark tpch.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2520.52ms │    2795.09ms │  1.11x slower │
│ QQuery 2     │     222.37ms │     216.01ms │     no change │
│ QQuery 3     │     248.41ms │     239.07ms │     no change │
│ QQuery 4     │     144.01ms │     129.28ms │ +1.11x faster │
│ QQuery 5     │     339.54ms │     327.53ms │     no change │
│ QQuery 6     │     147.59ms │     138.73ms │ +1.06x faster │
│ QQuery 7     │     605.72ms │     631.23ms │     no change │
│ QQuery 8     │     326.35ms │     372.12ms │  1.14x slower │
│ QQuery 9     │     579.02ms │     634.73ms │  1.10x slower │
│ QQuery 10    │     403.38ms │     420.39ms │     no change │
│ QQuery 11    │     201.94ms │     212.12ms │  1.05x slower │
│ QQuery 12    │     235.94ms │     254.58ms │  1.08x slower │
│ QQuery 13    │     738.40ms │     789.67ms │  1.07x slower │
│ QQuery 14    │     198.73ms │     206.96ms │     no change │
│ QQuery 15    │     183.32ms │     179.53ms │     no change │
│ QQuery 16    │     168.57ms │     186.43ms │  1.11x slower │
│ QQuery 17    │    2032.57ms │    2108.12ms │     no change │
│ QQuery 18    │    1912.80ms │    2134.82ms │  1.12x slower │
│ QQuery 19    │     391.64ms │     368.53ms │ +1.06x faster │
│ QQuery 20    │     648.22ms │     691.41ms │  1.07x slower │
│ QQuery 21    │     866.25ms │    1020.37ms │  1.18x slower │
│ QQuery 22    │     115.94ms │     117.27ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
--------------------
Benchmark tpch_mem.json
--------------------
┏━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┓
┃ Query        ┃         main ┃     mybranch ┃        Change ┃
┡━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━┩
│ QQuery 1     │    2182.44ms │    2390.39ms │  1.10x slower │
│ QQuery 2     │     181.16ms │     153.94ms │ +1.18x faster │
│ QQuery 3     │      98.89ms │      95.51ms │     no change │
│ QQuery 4     │      61.43ms │      66.15ms │  1.08x slower │
│ QQuery 5     │     260.20ms │     283.65ms │  1.09x slower │
│ QQuery 6     │      24.24ms │      23.39ms │     no change │
│ QQuery 7     │     545.87ms │     653.34ms │  1.20x slower │
│ QQuery 8     │     147.48ms │     136.00ms │ +1.08x faster │
│ QQuery 9     │     371.53ms │     363.61ms │     no change │
│ QQuery 10    │     197.91ms │     190.37ms │     no change │
│ QQuery 11    │     197.91ms │     183.70ms │ +1.08x faster │
│ QQuery 12    │     100.32ms │     103.08ms │     no change │
│ QQuery 13    │     428.02ms │     440.26ms │     no change │
│ QQuery 14    │      38.50ms │      27.11ms │ +1.42x faster │
│ QQuery 15    │     101.15ms │      63.25ms │ +1.60x faster │
│ QQuery 16    │     171.15ms │     142.44ms │ +1.20x faster │
│ QQuery 17    │    1885.05ms │    1953.58ms │     no change │
│ QQuery 18    │    1549.92ms │    1914.06ms │  1.23x slower │
│ QQuery 19    │     106.53ms │     104.28ms │     no change │
│ QQuery 20    │     532.11ms │     610.62ms │  1.15x slower │
│ QQuery 21    │     723.39ms │     823.34ms │  1.14x slower │
│ QQuery 22    │      91.84ms │      89.89ms │     no change │
└──────────────┴──────────────┴──────────────┴───────────────┘
```
### Running Benchmarks Manually

Assuming data is in the `data` directory, the `tpch` benchmark can be run with a command like this:

```bash
cargo run --release --bin embench -- tpch --iterations 3 --path ./data  --query 1
```

### Different features

You can enable `mimalloc` or `snmalloc` (to use either the mimalloc or snmalloc allocator) as features by passing them in as `--features`. For example:

```shell
cargo run --release --features "mimalloc" --bin embench -- tpch --iterations 3 --path ./data --query 1
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

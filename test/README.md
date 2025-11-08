[![SQL Logic Test Coverage](https://raw.githubusercontent.com/embucket/embucket-labs/assets/assets/badge.svg)](test/README.md)

## SLT coverage
![Test Coverage Visualization](https://raw.githubusercontent.com/embucket/embucket-labs/assets/assets/test_coverage_visualization.png)

![Not Implemented Tests Distribution](https://raw.githubusercontent.com/embucket/embucket-labs/assets/assets/not_implemented_visualization.png)

*These visualizations are automatically updated by CI/CD when tests are run.*

# SQL Logic Tests
We have a set of `.slt` files that represent our SQL Logic Tests. You can run them against Snowflake or Embucket.

# SLT Runner
1. Copy `.env_example` inside `slt_runner` folder file and rename it to `.env` file.
2. Set up a connection
   1. **Embucket** (default) - launch Embucket locally. The runner will automatically use `EMBUCKET_USER` and `EMBUCKET_PASSWORD` credentials (both set to `embucket` by default). Make sure connection parameters match Embucket launch parameters (if you have default settings, you don't need to change anything).
   2. **Snowflake** - replace Snowflake credentials (`SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`) in `.env` file with your actual credentials and set `EMBUCKET_ENABLED=false`.
3. Install requirements
``` bash
pip install -r slt_runner/requirements.txt
```
4. Run SLTs
``` bash
python -m slt_runner --test-file sql/sql-reference-commands/Query_syntax/select.slt
```
5. Parallel SLTs Execution
- Run each test file in a separate process:
   ```
   python -m slt_runner --test-dir sql --parallel
   ```
- Specify the number of parallel workers (defaults to number of CPUs):
   ```
  python -m slt_runner --test-dir sql --parallel --workers 4
   ```
6. Precision Mode

When running SQL Logic Tests, you can use precision mode to control how numeric values are compared:

- **Default behavior (no flag)**: Floating-point values are compared with relaxed precision using `math.isclose()` with:
  - Relative tolerance: 0.05 (5% of the larger value)
  - Absolute tolerance: 0.1
- **With `--precision` flag**: Exact string comparison for numeric values

This is useful for tests where exact decimal precision is required.

```bash
# Run tests with default precision (allows small differences in floating point values)
python -m slt_runner --test-file sql/your-test-file.slt

# Run tests with exact precision comparison
python -m slt_runner --test-file sql/your-test-file.slt --precision

You will see the `errors.log` and `test_statistics.csv` files generated. They contain errors and coverage statistics.
You can also visualize statistics using the `slt_runner/visualise_statistics.py` script.
You can also run all the tests in the folder using:
``` bash
python -m slt_runner --test-dir sql
```

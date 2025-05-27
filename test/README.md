[![SQL Logic Test Coverage](https://raw.githubusercontent.com/Embucket/embucket/assets/assets/badge.svg)](test/README.md)

## SLT coverage
![Test Coverage Visualization](https://raw.githubusercontent.com/Embucket/embucket/assets/assets/test_coverage_visualization.png)
*This visualization is automatically updated by CI/CD when tests are run.*

# SQL Logic Tests
We have a set of `.slt` files that represent our SQL Logic Tests. You can run them against Snowflake or Embucket.

# SLT Runner
1. Copy `.env_example` inside `slt_runner` folder file and rename it to `.env` file.
2. Set up a connection
   1. Snowflake - replace Snowflake credentials in `.env` file from `test` to your credentials.
   2. Embucket - launch Embucket locally, set `ICEBUCKET_ENABLED=true` in `.env` file, make sure connection parameters match Embucket launch parameters (if you have default settings, you don't need to change anything).
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
You will see the `errors.log` and `test_statistics.csv` files generated. They contain errors and coverage statistics.
You can also visualize statistics using the `slt_runner/visualise_statistics.py` script.
You can also run all the tests in the folder using:
``` bash
python -m slt_runner --test-dir sql
```

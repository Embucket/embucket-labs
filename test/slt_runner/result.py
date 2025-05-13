import datetime
import io
import json
import os
import re
import sys
import threading
import time
import typing
from enum import Enum
from functools import cmp_to_key
from hashlib import md5
from typing import Optional, Any, Tuple, List, Dict, Generator

from snowflake import connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import (
    FIELD_ID_TO_NAME,
    FIELD_NAME_TO_ID,
)
from snowflake.connector.converter import SnowflakeConverter
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor

from slt_runner import SkipIf, ExcludeFromCoverage
from slt_runner.base_statement import BaseStatement
from slt_runner.expected_result import ExpectedResult
from slt_runner.logger import logger
from slt_runner.my_token import TokenType
from slt_runner.statement import (
    Statement,
    Mode,
    Halt,
    Set,
    Load,
    Query,
    HashThreshold,
    Loop,
    Foreach,
    Endloop,
    Restart,
    Reconnect,
    Sleep,
    SleepUnit,
    Skip,
    SortStyle,
    Unskip,
    Stub,
)
from slt_runner.test import SQLLogicTest
from slt_runner.test_logger import SQLLogicTestLogger


### Helper structs

class RequireResult(Enum):
    MISSING = 0
    PRESENT = 1


class ExecuteResult:
    def __init__(self):
        # Updated to hold all results
        self.queries = []  # List of QueryExecuteResult objects


class QueryExecuteResult:
    def __init__(self, query, success, execution_time_s, exclude_from_coverage, error_message=None, skip=False):
        self.query = query
        self.success = success
        self.execution_time_s = execution_time_s
        self.skip = skip
        self.error_message = error_message  # None if no error
        self.query_id = None # store the query ID
        self.exclude_from_coverage = exclude_from_coverage


CONVERTER = SnowflakeConverter()


def format_if_json(value):
    try:
        # Attempt to parse the string as JSON
        json_obj = json.loads(value)
        # If successful, return a compact single-line JSON string
        return json.dumps(json_obj, separators=(',', ':'))
    except (ValueError, TypeError):  # Not a valid JSON string
        return value  # Return the original value if it's not JSON


class SQLLogicStatementData:
    # Context information about a statement
    def __init__(self, test: SQLLogicTest, statement: BaseStatement):
        self.test = test
        self.statement = statement

    def __str__(self) -> str:
        return f'{self.test.path}:{self.statement.get_query_line()}'

    __repr__ = __str__


class TestException(Exception):
    __slots__ = ['data', 'message']

    def __init__(self, data: SQLLogicStatementData, message: str):
        self.message = f'{message}'
        super().__init__(self.message)
        self.data = data


class SkipException(TestException):
    def __init__(self, data: SQLLogicStatementData, message: str):
        super().__init__(data, message)


class FailException(TestException):
    def __init__(self, data: SQLLogicStatementData, message: str):
        super().__init__(data, message)


### Result primitive

def get_column_metadata(result_metadata):
    return {
        "name": result_metadata.name,
        "type_code": result_metadata.type_code,
        "display_size": result_metadata.display_size,
        "internal_size": result_metadata.internal_size,
        "precision": result_metadata.precision,
        "scale": result_metadata.scale,
        "is_nullable": result_metadata.is_nullable,
    }


class QueryResult:
    def __init__(self, result: List[Tuple[Any]], types: List[str], error: Optional[Exception] = None, metadata: Optional[List[ResultMetadata]] = []):
        self._result = result
        self.types = types
        self.metadata = metadata # to be used with snowflake converter 
        self.error = error
        if not error:
            self._column_count = len(self.types)
            self._row_count = len(result)

    def get_value(self, column, row):
        return self._result[row][column]

    def row_count(self) -> int:
        return self._row_count

    @property
    def column_count(self) -> int:
        return self._column_count

    def has_error(self) -> bool:
        return self.error != None

    def get_error(self) -> Optional[Exception]:
        return self.error

    def check(self, context, query: Query) -> None:
        expected_column_count = query.expected_result.get_expected_column_count()
        expected_values = query.expected_result.lines
        logger.debug(f'check expected_result expected_values {expected_values}')
        sort_style = query.get_sortstyle()
        query_label = query.get_label()
        query_has_label = query_label != None
        runner = context.runner

        test_logger = SQLLogicTestLogger(context, query, runner.test.path)

        # If the result has an error, log it
        if self.has_error():
            error = self.get_error()
            error_msg = test_logger.unexpected_failure(error)

            if runner.skip_error_message(error):
                runner.finished_processing_file = True
                return
            logger.debug(error_msg)
            context.fail(error_msg)

        row_count = self.row_count()
        column_count = self.column_count
        total_value_count = row_count * column_count

        if len(expected_values) == 1 and result_is_hash(expected_values[0]):
            compare_hash = True
            is_hash = True
        else:
            compare_hash = query_has_label or (runner.hash_threshold > 0 and total_value_count > runner.hash_threshold)
            is_hash = False

        result_values_string = db_convert_result(self, runner.original_sqlite_test)

        if sort_style == SortStyle.ROW_SORT:
            ncols = self.column_count
            nrows = int(total_value_count / ncols)
            rows = [result_values_string[i * ncols : (i + 1) * ncols] for i in range(nrows)]

            # Define the comparison function
            def compare_rows(a, b):
                for col_idx, val in enumerate(a):
                    a_val = val
                    b_val = b[col_idx]
                    if a_val != b_val:
                        return -1 if a_val < b_val else 1
                return 0

            # Sort the individual rows based on element comparison
            sorted_rows = sorted(rows, key=cmp_to_key(compare_rows))
            rows = sorted_rows

            for row_idx, row in enumerate(rows):
                for col_idx, val in enumerate(row):
                    result_values_string[row_idx * ncols + col_idx] = val
        elif sort_style == SortStyle.VALUE_SORT:
            result_values_string.sort()

        hash_value = ""
        if runner.output_hash_mode or compare_hash:
            hash_context = md5()
            for val in result_values_string:
                hash_context.update(str(val).encode())
                hash_context.update("\n".encode())
            digest = hash_context.hexdigest()
            hash_value = f"{total_value_count} expected_values hashing to {digest}"
            if runner.output_hash_mode:
                test_logger.output_hash(hash_value)
                return

        if not compare_hash:
            original_expected_columns = expected_column_count
            column_count_mismatch = False

            if expected_column_count != self.column_count:
                expected_column_count = self.column_count
                column_count_mismatch = True

            try:
                expected_rows = len(expected_values) / expected_column_count
            except ZeroDivisionError:
                expected_rows = 0
            row_wise = expected_column_count > 1 and len(expected_values) == self.row_count()

            if not row_wise:
                all_tabs = all("\t" in val for val in expected_values)
                row_wise = all_tabs

            if row_wise:
                expected_rows = len(expected_values)
                row_wise = True
            elif len(expected_values) % expected_column_count != 0:
                if column_count_mismatch:
                    error_msg = test_logger.column_count_mismatch(self, expected_values, original_expected_columns)
                else:
                    error_msg = test_logger.not_cleanly_divisible(expected_column_count, len(expected_values))
                context.fail(error_msg)

            if expected_rows != self.row_count():
                if column_count_mismatch:
                    error_msg = test_logger.column_count_mismatch(self, expected_values, original_expected_columns)
                else:
                    error_msg = test_logger.wrong_row_count(expected_rows, result_values_string, expected_values,
                                                            expected_column_count, row_wise)
                context.fail(error_msg)

            # iterate over rows (expected_values)
            for current_row, row_data in enumerate(expected_values):
                row = [x for x in row_data.split("\t") if x != ''] if row_wise else row_data
                row = row if type(row) is list else [row]
                # row should be type of list
                if row_wise and len(row) != expected_column_count:
                    error_msg = ""
                    if column_count_mismatch:
                        error_msg += test_logger.column_count_mismatch(self, expected_values, original_expected_columns)
                    error_msg += test_logger.split_mismatch(current_row + 1, expected_column_count, len(row))
                    context.fail(error_msg)
                for current_column, column_value in enumerate(row):
                    try:
                        lvalue_str = result_values_string[current_row * len(row) + current_column]
                        rvalue_str = column_value
                    except Exception as e:
                        logger.error(f'current_row: {current_row}, current_column:{current_column}')
                        raise e
                    success = compare_values(self, lvalue_str, rvalue_str, current_column)
                    if not success:
                        msg = '\n'.join([
                            f"Mismatch on row {current_row + 1}, column {current_column + 1}",
                            f"{lvalue_str} <> {rvalue_str}",
                        ])
                        # row_wise means expected data is array of string rows, with tabs as
                        # columns separator; resulted data is always an usual two-dimensional array
                        # thus row_wise used for correct displaying expected data
                        error_msg = test_logger.wrong_result_query(
                            expected_values, result_values_string,
                            expected_column_count, msg, row_wise
                        )
                        context.fail(error_msg)
                    assert success

            if column_count_mismatch:
                error_msg = test_logger.column_count_mismatch_correct_result(original_expected_columns,
                                                                             expected_column_count, self)
                context.fail(error_msg)
        else:
            hash_compare_error = False
            if query_has_label:
                entry = runner.hash_label_map.get(query_label)
                if entry is None:
                    runner.hash_label_map[query_label] = hash_value
                    runner.result_label_map[query_label] = self
                else:
                    hash_compare_error = entry != hash_value

            if is_hash:
                hash_compare_error = expected_values[0] != hash_value

            if hash_compare_error:
                expected_result = runner.result_label_map.get(query_label)
                error_msg = test_logger.wrong_result_hash(expected_result, self)
                error_msg += f'\nHash compare error in query: {query}'
                context.fail_query(error_msg)

            assert not hash_compare_error


class SQLLogicConnectionPool:
    __slots__ = [
        'connection',
        'cursors',
    ]

    def __init__(self, con: SnowflakeConnection):
        assert con
        self.cursors = {}
        self.connection = con

    def initialize_connection(self, con):
        try:
            con.execute("SET timezone='UTC'")
        except Exception:
            pass

    def get_connection(self, name: Optional[str] = None) -> SnowflakeConnection | None:
        """
        Fetch the 'self.connection' object if name is None
        """
        assert self.connection
        if name is None:
            return self.connection



class SQLLogicDatabase:
    __slots__ = ['path', 'config', 'database']

    def __init__(
        self, path: str, config: Dict[str, str], context: Optional["SQLLogicContext"] = None
    ):
        """
        Connection Hierarchy:

        database
        └── connection
            └── cursor1
            └── cursor2
            └── cursor3

        'connection' is a cursor of 'database'.
        Every entry of 'cursors' is a cursor created from 'connection'.

        This is important to understand how ClientConfig settings affect each cursor.
        """
        self.reset()
        if config:
            self.config.update(config)
        self.path = path

        # Now re-open the current database
        if 'access_mode' not in self.config:
            self.config['access_mode'] = 'automatic'
        self.database = connector.connect(**self.config)

    def reset(self):
        self.database: Optional[SnowflakeConnection] = None
        self.config: Dict[str, Any] = {
            'allow_unsigned_extensions': True,
            'allow_unredacted_secrets': True,
        }
        self.path = ''

    def connect(self) -> SQLLogicConnectionPool:
        return SQLLogicConnectionPool(self.database.cursor())


def is_regex(input: str) -> bool:
    return input.startswith("<REGEX>:") or input.startswith("<!REGEX>:")


def matches_regex(input: str, actual_str: str) -> bool:
    if input.startswith("<REGEX>:"):
        should_match = True
        regex_str = input.replace("<REGEX>:", "")
    else:
        should_match = False
        regex_str = input.replace("<!REGEX>:", "")

    re_options = re.DOTALL
    re_pattern = re.compile(regex_str, re_options)
    regex_matches = bool(re_pattern.fullmatch(actual_str))
    return regex_matches == should_match


def compare_values(result: QueryResult, actual_str, expected_str, current_column):
    error = False

    if actual_str == expected_str:
        return True

    if is_regex(expected_str):
        return matches_regex(expected_str, actual_str)

    col = result.metadata[current_column]
    logger.debug(f'compare_values::current_column: {current_column}')
    sql_type = result.types[current_column]

    def is_numeric(type) -> bool:
        from snowflake.connector.constants import is_number_type_name
        return is_number_type_name(str(type))

    if is_numeric(sql_type) or sql_type == 'BOOLEAN' or sql_type == 'TIMESTAMP_TZ':
        expected = convert_value2(expected_str, sql_type)
        actual = convert_value2(actual_str, sql_type)
        return expected == actual
    expected = expected_str
    actual = actual_str
    error = actual != expected

    if error:
        return False
    return True


def result_is_hash(result):
    parts = result.split()
    if len(parts) != 5:
        return False
    if not parts[0].isdigit():
        return False
    if parts[1] != "values" or parts[2] != "hashing" or len(parts[4]) != 32:
        return False
    return all([x.islower() or x.isnumeric() for x in parts[4]])


def convert_value2(value, sql_type):
    col = {"type_code": FIELD_NAME_TO_ID[sql_type.upper()]}
    logger.debug(f'convert_value2: {value}:{type(value)}, type_code: {col["type_code"]} {sql_type}')
    converter_method = CONVERTER.to_python_method(sql_type.upper(), col)
    if converter_method is None:
        return value
    return converter_method(value)

def convert_value(value, col: ResultMetadata):
    if value is None or value == 'NULL':
        return 'NULL'
    elif isinstance(value, bytearray) or isinstance(value, bytes):  # Binary data
        return f"x'{value.hex()}'"  # Convert to SQL hex literal (e.g., x'1010')
    elif isinstance(value, datetime.date) or isinstance(value, datetime.time):
        return f"'{value.isoformat()}'"
    elif isinstance(value, list) or isinstance(value, dict):
        return f"'{json.dumps(value, separators=(',', ':'))}'"
    elif isinstance(value, str):  # Strings
        if value in {"true", "false", "null"}:
            # Return them as plain strings without parsing
            return value
        try:
            value = value.replace("undefined", "null")
            parsed_json = json.loads(value)
            if isinstance(parsed_json, (dict, list)):
                return f"'{json.dumps(parsed_json, separators=(',', ':'))}'"
            else:
                return value.replace('\n', '\\n')
        except (ValueError, TypeError):
            # If it's not JSON, return the string as it is
            if value == "":
                return "''"
            return value.replace('\n', '\\n')
    col_asdict = {'name': col.name, 'type_code': col.type_code, 'scale': col.scale}
    sql_type = FIELD_ID_TO_NAME[col.type_code]
    logger.debug(f'convert_value from: {value}:{type(value).__name__}, type_code: {col.type_code} {sql_type}')
    if type(value) is bool and sql_type == 'BOOLEAN':
        logger.debug('skip bool value to_python conversion: already bool')
        return value
    converter_method = CONVERTER.to_python_method(sql_type, col_asdict)
    res = value
    if converter_method is None:
        logger.debug('converted_method is None')
        if sql_type.upper() in ('ARRAY', 'VARIANT'):
            res = format_if_json(value)
    else:
        try:
            res = converter_method(value)
        except Exception as e:
            logger.error(repr(e))
            raise e
    logger.debug(f'converted to: {res}')
    return res

def to_test_str(value):
    if type(value) is bool:
        return 'TRUE' if value is True else 'FALSE'
    else:
        return str(value)

def sql_logic_test_convert_value(value, col, is_sqlite_test: bool) -> str:
    if value is None or value == 'NULL':
        return 'NULL'
    sql_type = FIELD_ID_TO_NAME[col.type_code]

    if sql_type == 'BOOLEAN':
        return "1" if convert_value(value, col) else "0"
    else:
        # res = convert_value2(value, 'TEXT')
        # Since we don't use database for converting values into str ...
        res = to_test_str(convert_value(value, col))
        if len(res) == 0:
            res = "(empty)"
        else:
            res = res.replace("\0", "\\0")
    return res


def db_convert_result(result: QueryResult, is_sqlite_test: bool) -> List[str]:
    out_result = []
    row_count = result.row_count()
    column_count = result.column_count

    for r in range(row_count):
        for c in range(column_count):
            col = result.metadata[c]
            value = result.get_value(c, r)
            out_result.append(value)

    return out_result

def error_from_exception_snowflake_connector(msg: str):
    # Since msg from snowflake connector have several possible outputs, eat exception
    try:
        return json.loads(
            msg.replace('"', '\\"').replace("\\'", '"').replace("'", '"')
        )['M']
    except Exception as e:
        return msg

class SQLLogicRunner:
    __slots__ = [
        'skipped',
        'error',
        'skip_level',
        'loaded_databases',
        'database',
        'extensions',
        'environment_variables',
        'test',
        'hash_threshold',
        'hash_label_map',
        'result_label_map',
        'required_requires',
        'output_hash_mode',
        'output_result_mode',
        'debug_mode',
        'finished_processing_file',
        'ignore_error_messages',
        'always_fail_error_messages',
        'original_sqlite_test',
        'build_directory',
        'skip_reload',  # <-- used for 'force_reload' and 'force_storage', unused for now
    ]

    def reset(self):
        self.skip_level: int = 0

        # The set of databases that have been loaded by this runner at any point
        # Used for cleanup
        self.loaded_databases: typing.Set[str] = set()
        self.database: Optional[SQLLogicDatabase] = None
        self.environment_variables: Dict[str, str] = {}
        self.test: Optional[SQLLogicTest] = None

        self.hash_threshold: int = 0
        self.hash_label_map: Dict[str, str] = {}
        self.result_label_map: Dict[str, Any] = {}

        # FIXME: create a CLI argument for this
        self.required_requires: set = set()
        self.output_hash_mode = False
        self.output_result_mode = False
        self.debug_mode = False

        self.finished_processing_file = False
        # If these error messages occur in a test, the test will abort but still count as passed
        self.ignore_error_messages = {"HTTP", "Unable to connect"}
        # If these error messages occur in a statement that is expected to fail, the test will fail
        self.always_fail_error_messages = {"differs from original result!", "INTERNAL"}

        self.original_sqlite_test = False

    def skip_error_message(self, message):
        for error_message in self.ignore_error_messages:
            if error_message in str(message):
                return True
        return False

    def __init__(self, config: Dict[str,str], default_test_dir: Optional[str] = None):
        self.reset()
        self.config = config
        self.default_test_dir = default_test_dir

    def skip(self):
        self.skip_level += 1

    def unskip(self):
        self.skip_level -= 1

    def skip_active(self) -> bool:
        return self.skip_level > 0

    def is_embucket(self):
        return 'embucket' in self.config


class SQLLogicContext:
    __slots__ = [
        'iterator',
        'runner',
        'generator',
        'STATEMENTS',
        'pool',
        'statements',
        'current_statement',
        'keywords',
        'error',
        'is_loop',
        'is_parallel',
        'build_directory',
        'cached_config_settings',
    ]

    def reset(self):
        self.iterator = 0

    def replace_keywords(self, input: str):
        # Apply a replacement for every registered keyword
        if '__BUILD_DIRECTORY__' in input:
            self.skiptest("Test contains __BUILD_DIRECTORY__ which isnt supported")
        for key, value in self.keywords.items():
            input = input.replace(key, value)
        return input


    def __init__(
        self,
        pool: SQLLogicConnectionPool,
        runner: SQLLogicRunner,
        statements: List[BaseStatement],
        keywords: Dict[str, str],
        iteration_generator,
    ):
        self.statements = statements
        self.runner = runner
        self.is_loop = True
        self.is_parallel = False
        self.error: Optional[TestException] = None
        self.generator: Generator[Any] = iteration_generator
        self.keywords = keywords
        self.cached_config_settings: List[Tuple[str, str]] = []
        self.current_statement: Optional[SQLLogicStatementData] = None
        self.pool: Optional[SQLLogicConnectionPool] = pool
        self.STATEMENTS = {
            Query: self.execute_query,
            Statement: self.execute_statement,
            Load: self.execute_stub,
            Skip: self.execute_skip,
            Unskip: self.execute_unskip,
            Mode: self.execute_mode,
            Sleep: self.execute_sleep,
            Reconnect: self.execute_reconnect,
            Halt: self.execute_halt,
            Restart: self.execute_stub,
            HashThreshold: self.execute_hash_threshold,
            Set: self.execute_set,
            Loop: self.execute_loop,
            Foreach: self.execute_foreach,
            Endloop: None,  # <-- should never be encountered outside of Loop/Foreach
            Stub: self.execute_stub,
        }

    def add_keyword(self, key, value):
        # Make sure that loop names can't silently collide
        key = f'${{{key}}}'
        assert key not in self.keywords
        self.keywords[key] = str(value)

    def remove_keyword(self, key):
        key = f'${{{key}}}'
        assert key in self.keywords
        self.keywords.pop(key)

    def fail(self, message):
        self.error = FailException(self.current_statement, message)
        raise self.error

    def skiptest(self, message: str):
        self.error = SkipException(self.current_statement, message)
        raise self.error

    def fail_query(self, message):
        self.error = TestException(
            self.current_statement, message
        )
        raise self.error

    def get_connection(self, name: Optional[str] = None) -> SnowflakeConnection:
        return self.pool.get_connection(name)


    def execute_query(self, query: Query):
        assert isinstance(query, Query)
        conn = self.get_connection(query.connection_name)
        sql_query = query.get_one_liner()
        sql_query = self.replace_keywords(sql_query)

        expected_result = query.expected_result
        assert expected_result.type == ExpectedResult.Type.SUCCESS

        try:
            def get_query_result(rel, results) -> QueryResult:
                """Convert result value to str and use this instead of original result, return QueryResult """
                if rel is None:
                    return QueryResult([(0,)], ['BIGINT'], None)
                else:
                    # We create new names for the columns, because they might be duplicated
                    aliased_columns = [f'c{i}' for i in range(len(rel.description))]
                    results_stringified = [[to_test_str(convert_value(v, rel.description[cn])) for cn, v in enumerate(r)] for r in results]
                    return QueryResult(results_stringified, aliased_columns, None, rel.description)

            cursor = conn.execute(sql_query, _no_retry=self.runner.is_embucket())
            result = cursor.fetchall()  # call fetchall() on the cursor object
            logger.debug(f'result: {result}')
            query_result = get_query_result(cursor, result)

            logger.debug(f"query_result {query_result.error} : {query_result.types}, {query_result._result}")

            if expected_result.lines == None:
                return
        except TestException as te:
            logger.debug(te.message)
            if not query_result:
                query_result = QueryResult([], [], te)
            raise te
        except Exception as e:
            logger.debug(e)
            query_result = QueryResult([], [], e)

        try:
            query_result.check(self, query)
        except TestException as e:
            raise e

    def execute_skip(self, statement: Skip):
        self.runner.skip()

    def execute_unskip(self, statement: Unskip):
        self.runner.unskip()

    def execute_stub(self, statement: Stub):
        st = f"{statement.header.text} {' '.join(statement.header.parameters)}"
        print(f"Skip unsupported SLT statement: {st}")

    def execute_halt(self, statement: Halt):
        self.skiptest("HALT was encountered in file")

    def execute_set(self, statement: Set):
        option = statement.header.parameters[0]
        string_set = (
            self.runner.ignore_error_messages
            if option == "ignore_error_messages"
            else self.runner.always_fail_error_messages
        )
        string_set.clear()
        string_set = statement.error_messages

    def execute_hash_threshold(self, statement: HashThreshold):
        self.runner.hash_threshold = statement.threshold

    def execute_reconnect(self, statement: Reconnect):
        if self.is_parallel:
            self.fail("reconnect can not be used inside a parallel loop")
        self.pool = None
        self.pool = self.runner.database.connect()
        con = self.pool.get_connection()
        self.pool.initialize_connection(self, con)

    def execute_sleep(self, statement: Sleep):
        def calculate_sleep_time(duration: float, unit: SleepUnit) -> float:
            if unit == SleepUnit.SECOND:
                return duration
            elif unit == SleepUnit.MILLISECOND:
                return duration / 1000
            elif unit == SleepUnit.MICROSECOND:
                return duration / 1000000
            elif unit == SleepUnit.NANOSECOND:
                return duration / 1000000000
            else:
                raise ValueError("Unknown sleep unit")

        unit = statement.get_unit()
        duration = statement.get_duration()

        time_to_sleep = calculate_sleep_time(duration, unit)
        time.sleep(time_to_sleep)

    def execute_mode(self, statement: Mode):
        parameter = statement.header.parameters[0]
        if parameter == "output_hash":
            self.runner.output_hash_mode = True
        elif parameter == "output_result":
            self.runner.output_result_mode = True
        elif parameter == "no_output":
            self.runner.output_hash_mode = False
            self.runner.output_result_mode = False
        elif parameter == "debug":
            self.runner.debug_mode = True
        else:
            raise RuntimeError("unrecognized mode: " + parameter)

    def execute_statement(self, statement: Statement):
        assert isinstance(statement, Statement)
        conn = self.get_connection(statement.connection_name)
        sql_query = statement.get_one_liner()
        sql_query = self.replace_keywords(sql_query)

        expected_result = statement.expected_result
        test_logger = SQLLogicTestLogger(self, statement, self.runner.test.path)

        try:
            conn.execute(sql_query, _no_retry=self.runner.is_embucket())

            # TODO refactor: encapsulate logic, check if embucket_enabled: False
            # storing executed statements
            if self.runner.benchmark_mode:
                self.runner.capture_query_id(conn)

            result = conn.fetchall()
            if expected_result.type != ExpectedResult.Type.UNKNOWN:
                if statement.header.type == TokenType.SQLLOGIC_STATEMENT and \
                        expected_result.lines is not None:
                    self.fail("'statement ok' doesn't expect results section: '----'")
                assert expected_result.lines == None
        except Exception as e:
            if expected_result.type == ExpectedResult.Type.SUCCESS:
                error_msg = test_logger.unexpected_failure(e)
                self.fail(error_msg)
            if expected_result.lines == None:
                return

            expected = '\n'.join(expected_result.lines)
            if is_regex(expected):
                if not matches_regex(expected, str(e)):
                    error_msg = test_logger.expected_failure_with_wrong_error(expected, e)
                    self.fail(error_msg)
            else:
                # Sanitize the expected error
                sanitized_expected = expected
                if expected.startswith('Dependency Error: '):
                    sanitized_expected = expected.split('Dependency Error: ')[1]

                error_message = ' '.join(str(e).split())  # Normalize whitespace & newlines
                sanitized_expected = ' '.join(sanitized_expected.split())  # Normalize expected error too

                if sanitized_expected not in error_message:
                    error_msg = test_logger.expected_failure_with_wrong_error(expected, e)
                    self.fail(error_msg)

    def get_loop_statements(self):
        saved_iterator = self.iterator
        # Loop until EndLoop is found
        statement = None
        depth = 0
        while self.iterator < len(self.statements):
            statement = self.next_statement()
            if statement.__class__ in [Foreach, Loop]:
                depth += 1
            if statement.__class__ == Endloop:
                if depth == 0:
                    break
                depth -= 1
        if not statement or statement.__class__ != Endloop:
            raise Exception("no corresponding 'endloop' found before the end of the file!")
        statements = self.statements[saved_iterator : self.iterator - 1]
        return statements

    def execute_parallel(self, context: "SQLLogicContext", key, value):
        context.is_parallel = True
        try:
            # For some reason the lambda won't capture the 'value' when created outside of 'execute_parallel'
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                context.add_keyword(key, value)
                yield None
                context.remove_keyword(key)

            context.generator = update_value
            context.execute()
        except TestException:
            assert context.error is not None

    def execute_loop(self, loop: Loop):
        statements = self.get_loop_statements()

        if not loop.parallel:
            # Every iteration the 'value' of the loop key needs to change
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                key = loop.name
                for val in range(loop.start, loop.end):
                    context.add_keyword(key, val)
                    yield None
                    context.remove_keyword(key)

            loop_context = SQLLogicContext(
                self.pool,
                self.runner,
                statements,
                self.keywords.copy(),
                update_value,
            )
            try:
                loop_context.execute()
            except TestException:
                self.error = loop_context.error
        else:
            contexts: Dict[Tuple[str, int], Any] = {}
            for val in range(loop.start, loop.end):
                # FIXME: these connections are expected to have the same settings
                # So we need to apply the cached settings to them
                contexts[(loop.name, val)] = SQLLogicContext(
                    self.runner.database.connect(),
                    self.runner,
                    statements,
                    self.keywords.copy(),
                    None,  # generator, can't be created yet
                )

            threads = []
            for keyval, context in contexts.items():
                key, value = keyval
                t = threading.Thread(target=self.execute_parallel, args=(context, key, value))
                threads.append(t)
                t.start()

            for thread in threads:
                thread.join()

            for _, context in contexts.items():
                if context.error is not None:
                    # Propagate the exception
                    self.error = context.error
                    raise self.error

    def execute_foreach(self, foreach: Foreach):
        statements = self.get_loop_statements()

        if not foreach.parallel:
            # Every iteration the 'value' of the loop key needs to change
            def update_value(context: SQLLogicContext) -> Generator[Any, Any, Any]:
                loop_keys = foreach.name.split(',')

                for val in foreach.values:
                    if len(loop_keys) != 1:
                        values = val.split(',')
                    else:
                        values = [val]
                    assert len(values) == len(loop_keys)
                    for i, key in enumerate(loop_keys):
                        context.add_keyword(key, values[i])
                    yield None
                    for key in loop_keys:
                        context.remove_keyword(key)

            loop_context = SQLLogicContext(
                self.pool,
                self.runner,
                statements,
                self.keywords.copy(),
                update_value,
            )
            loop_context.execute()
        else:
            # parallel loop: launch threads
            contexts: List[Tuple[str, int, Any]] = []
            loop_keys = foreach.name.split(',')
            for val in foreach.values:
                if len(loop_keys) != 1:
                    values = val.split(',')
                else:
                    values = [val]

                assert len(values) == len(loop_keys)
                for i, key in enumerate(loop_keys):
                    contexts.append(
                        (
                            foreach.name,
                            values[i],
                            SQLLogicContext(
                                self.runner.database.connect(),
                                self.runner,
                                statements,
                                self.keywords.copy(),
                                None,  # generator, can't be created yet
                            ),
                        )
                    )

            threads = []
            for x in contexts:
                key, value, context = x
                t = threading.Thread(target=self.execute_parallel, args=(context, key, value))
                threads.append(t)
                t.start()

            for thread in threads:
                thread.join()

            for x in contexts:
                _, _, context = x
                if context.error is not None:
                    self.error = context.error
                    raise self.error

    def next_statement(self):
        if self.iterator >= len(self.statements):
            raise Exception("'next_statement' out of range, statements already consumed")
        statement = self.statements[self.iterator]
        self.iterator += 1
        return statement

    def verify_statements(self) -> None:
        unsupported_statements = [
            statement for statement in self.statements if statement.__class__ not in self.STATEMENTS.keys()
        ]
        if unsupported_statements == []:
            return
        types = set([x.__class__ for x in unsupported_statements])
        error = f'skipped because the following statement types are not supported: {str(list([x for x in types]))}'
        self.skiptest(error)

    def execute(self):
        results = ExecuteResult()  # Create a result container
        for _ in self.generator(self):
            self.reset()
            while self.iterator < len(self.statements):
                statement = self.next_statement()
                # Skip
                self.current_statement = SQLLogicStatementData(self.runner.test, statement)
                if self.runner.skip_active() and statement.__class__ != Unskip:
                    # Keep skipping until Unskip is found
                    continue
                exclude_from_coverage = False
                if statement.get_decorators() != []:
                    decorator = statement.get_decorators()[0]
                    if isinstance(decorator, SkipIf):
                        results.queries.append(
                            QueryExecuteResult(statement.get_one_liner(), success=False, execution_time_s=0, skip=True)
                        )
                        continue
                    elif isinstance(decorator, ExcludeFromCoverage):
                        exclude_from_coverage = True
                    else:
                        self.skiptest(f"Decorators {decorator.token.text} is not supported")
                method = self.STATEMENTS.get(statement.__class__)
                if not method:
                    self.skiptest("Not supported by the runner")
                logger.debug(f"statement type: {statement.header.type}")
                if isinstance(statement, Statement) or isinstance(statement, Query):
                    logger.debug(f"statement sql: {statement.get_query_line()}")
                try:
                    start_time = time.time()
                    method(statement)
                    # If successful, log individual query as SUCCESS
                    results.queries.append(QueryExecuteResult(statement.get_query_line(), execution_time_s=time.time() - start_time, success=True, exclude_from_coverage=exclude_from_coverage))
                except TestException as e:
                    # If execution fails, log the failure for the current query
                    results.queries.append(
                        QueryExecuteResult(statement.get_query_line(), execution_time_s=time.time() - start_time, success=False, error_message=e.message, exclude_from_coverage=exclude_from_coverage)
                    )

        return results

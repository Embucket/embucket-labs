import logging
import termcolor
from typing import Union
from slt_runner.statement import Query, Statement
from slt_runner.logger import logger

class SQLLogicTestLogger:
    def __init__(self, context, command: Union[Query, Statement], file_name: str):
        self.file_name = file_name
        self.context = context
        self.query_line = command.query_line
        self.sql_query = command.get_one_liner()

    def _build_output(self, parts):
        """Build output string from parts list"""
        return '\n'.join(parts) if isinstance(parts, list) else str(parts)


    def print_expected_result(self, values, columns, row_wise):
        logger.debug(f'{columns}, {values}')
        if row_wise:
            for value in values:
                print(value)
        else:
            c = 0
            for value in values:
                if c != 0:
                    print("\t", end="")
                print(value, end="")
                c += 1
                if c >= columns:
                    c = 0
                    print()

    def print_line_sep(self):
        line_sep = "=" * 80
        return termcolor.colored(line_sep, 'grey')

    def print_header(self, header):
        return termcolor.colored(header, 'white', attrs=['bold'])

    def print_file_header(self):
        self.print_header(f"File {self.file_name}:{self.query_line})")

    def get_sql(self) -> str:
        query = self.sql_query.strip()
        if not query.endswith(";"):
            query += ";"
        return query

    def print_sql(self):
        return self.get_sql()


    def print_error_header(self, description):
        parts = []
        parts.append("")
        parts.append(self.print_line_sep())
        parts.append(
            f"{termcolor.colored(description, 'red', attrs=['bold'])} {termcolor.colored(f'({self.file_name}:{self.query_line})!', attrs=['bold'])}")
        return '\n'.join(parts)

    def unexpected_failure(self, error):
        parts = []
        parts.append(self.print_error_header(f"Query unexpectedly failed!"))
        parts.append(self.print_line_sep())
        parts.append(str(error))
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def expected_failure_with_wrong_error(self, expected_error, actual_error):
        parts = []
        parts.append(self.print_error_header(f"Query failed with unexpected error!"))
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        parts.append('Expected error:')
        parts.append(str(expected_error))
        parts.append(self.print_line_sep())
        parts.append('Actual error:')
        parts.append(str(actual_error))
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def no_error_but_expected(self, expected_error):
        parts = []
        parts.append(self.print_error_header(f"Query did not fail, but expected error!"))
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        parts.append('Expected error:')
        parts.append(str(expected_error))
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def output_hash(self, hash_value):
        parts = []
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        parts.append(hash_value)
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def column_count_mismatch(self, result, result_values_string, expected_column_count):
        parts = []
        parts.append(self.print_error_header("Wrong column count in query!"))
        parts.append(
            f"Expected {termcolor.colored(expected_column_count, 'white', attrs=['bold'])} columns, but got {termcolor.colored(result.column_count, 'white', attrs=['bold'])} columns"
        )
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())

        # Add expected result section
        parts.append(self.print_header("Expected result:"))

        if result_values_string:
            # For list of strings with tab-separated values (actual format)
            parts.extend(result_values_string)
        else:
            parts.append(f"(Expected schema with {expected_column_count} columns)")
            parts.append("(No expected values provided)")

        # Add line separator
        parts.append(self.print_line_sep())

        # Add actual result section
        parts.append(self.print_header("Actual result:"))

        if result._result and len(result._result) > 0:
            # Format actual results
            actual_lines = ['\t'.join(r) for r in result._result]
            parts.extend(actual_lines)

        else:
            parts.append("(No result rows)")
            parts.append(self.print_line_sep())

        parts.append("")
        return '\n'.join(parts)

    def not_cleanly_divisible(self, expected_column_count, actual_column_count):
        parts = []
        parts.append(self.print_error_header("Error in test!"))
        parts.append(f"Expected {expected_column_count} columns, but {actual_column_count} values were supplied")
        parts.append("This is not cleanly divisible (i.e. the last row does not have enough values)")
        return '\n'.join(parts)

    def wrong_row_count(self, expected_rows, result_values_string, comparison_values, expected_column_count, row_wise):
        parts = []
        parts.append(self.print_error_header("Wrong row count in query!"))
        row_count = len(result_values_string)
        parts.append(
            f"Expected {termcolor.colored(int(expected_rows), 'white', attrs=['bold'])} rows, but got {termcolor.colored(row_count, 'white', attrs=['bold'])} rows"
        )
        parts.append(self.print_line_sep())
        parts.append(self.print_sql())
        parts.append(self.print_line_sep())

        # Add expected results
        parts.append(self.print_header("Expected result:"))
        expected_output = []
        self._format_expected_result(expected_output, comparison_values, expected_column_count, row_wise)
        parts.extend(expected_output)

        # Add actual results
        parts.append(self.print_line_sep())
        parts.append(self.print_header("Actual result:"))
        actual_output = []
        self._format_expected_result(actual_output, result_values_string, expected_column_count, False)
        parts.extend(actual_output)
        parts.append(self.print_line_sep())


        return '\n'.join(parts)

    # Helper for formatting results without printing
    def _format_expected_result(self, output_list, values, columns, row_wise):
        if row_wise:
            for value in values:
                output_list.append(value)
        else:
            line = ""
            c = 0
            for value in values:
                if c != 0:
                    line += "\t"
                line += str(value)
                c += 1
                if c >= columns:
                    c = 0
                    output_list.append(line)
                    line = ""
            if line:
                output_list.append(line)

    def column_count_mismatch_correct_result(self, original_expected_columns, expected_column_count, result):
        parts = []
        parts.append(self.print_line_sep())
        parts.append(self.print_error_header("Wrong column count in query!"))
        parts.append(
            f"Expected {termcolor.colored(original_expected_columns, 'white', attrs=['bold'])} columns, but got {termcolor.colored(expected_column_count, 'white', attrs=['bold'])} columns"
        )
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(f"The expected result {termcolor.colored('matched', 'white', attrs=['bold'])} the query result.")
        parts.append(
            f"Suggested fix: modify header to \"{termcolor.colored('query', 'green')} {'I' * result.column_count}{termcolor.colored('', 'white')}\""
        )
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def split_mismatch(self, row_number, expected_column_count, split_count):
        parts = []
        parts.append(self.print_line_sep())
        parts.append(self.print_error_header(
            f"Error in test! Column count mismatch after splitting on tab on row {row_number}!"))
        parts.append(
            f"Expected {termcolor.colored(int(expected_column_count), 'white', attrs=['bold'])} columns, but got {termcolor.colored(split_count, 'white', attrs=['bold'])} columns"
        )
        parts.append("Does the result contain tab values? In that case, place every value on a single row.")
        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def wrong_result_query(self, expected_values, result_values_string, expected_column_count, err_msg, row_wise):
        parts = []
        parts.append(self.print_error_header("Wrong result in query!"))
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        parts.append(err_msg)
        parts.append(self.print_line_sep())

        # Format expected results
        parts.append(self.print_header("Expected result:"))
        expected_output = []
        self._format_expected_result(expected_output, expected_values, expected_column_count, row_wise)
        parts.extend(expected_output)

        # Format actual results
        parts.append(self.print_line_sep())
        parts.append(self.print_header("Actual result:"))
        actual_output = []
        self._format_expected_result(actual_output, result_values_string, expected_column_count, False)
        parts.extend(actual_output)

        parts.append(self.print_line_sep())
        return '\n'.join(parts)

    def wrong_result_hash(self, expected_result, result):
        parts = []
        if expected_result:
            parts.append(str(expected_result))
        else:
            parts.append("???")
        parts.append(self.print_error_header("Wrong result hash!"))
        parts.append(self.print_line_sep())
        parts.append(self.get_sql())
        parts.append(self.print_line_sep())
        parts.append(self.print_header("Expected result:"))
        parts.append(self.print_header("Actual result:"))
        return '\n'.join(parts)

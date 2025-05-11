import sqlglot
from slt_runner.logger import logger
from slt_runner.base_statement import BaseStatement
from slt_runner.expected_result import ExpectedResult
from slt_runner.my_token import Token
from typing import List, Optional

class BaseSqlStatement(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.lines: List[str] = []
        self.expected_result: Optional[ExpectedResult] = None
        self.connection_name: Optional[str] = None
        # optinal - converted query
        self.derivative_query = None
   
    def set_connection(self, connection: str):
        self.connection_name = connection

    def add_lines(self, lines: List[str]):
        self.lines.extend(lines)

    def set_expected_result(self, expected_result: ExpectedResult):
        self.expected_result = expected_result

    def get_one_liner(self) -> str:
        return self.derivative_query if self.derivative_query != None else '\n'.join(self.lines)
    
    def set_derivative_query(self, from_dialect):
        if from_dialect != None:
            oneliner = self.get_one_liner()
            try:
                self.derivative_query = sqlglot.transpile(
                    oneliner, 
                    read=from_dialect, 
                    write="snowflake", 
                )[0]
            except Exception as e:
                self.derivative_query = None
                # do not raise Exception as sql transpile is just an option
                logger.error(f"SQL '{oneliner}' transpile error: {e}")
            logger.debug(f'transpiled: {self.derivative_query}')

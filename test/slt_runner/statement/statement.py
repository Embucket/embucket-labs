from slt_runner.base_sql_statement import BaseSqlStatement
from slt_runner.my_token import Token

class Statement(BaseSqlStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)

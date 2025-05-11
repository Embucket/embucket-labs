from slt_runner.base_statement import BaseStatement
from slt_runner.my_token import Token


class Mode(BaseStatement):
    def __init__(self, header: Token, line: int, parameter: str):
        super().__init__(header, line)
        self.parameter = parameter

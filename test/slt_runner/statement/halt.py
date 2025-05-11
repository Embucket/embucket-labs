from slt_runner.base_statement import BaseStatement
from slt_runner.my_token import Token


class Halt(BaseStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)

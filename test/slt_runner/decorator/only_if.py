from slt_runner.base_decorator import BaseDecorator
from slt_runner.my_token import Token


class OnlyIf(BaseDecorator):
    def __init__(self, token: Token):
        super().__init__(token)

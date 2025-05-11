from slt_runner.base_decorator import BaseDecorator
from slt_runner.my_token import Token


class ExcludeFromCoverage(BaseDecorator):
    def __init__(self, token: Token):
        super().__init__(token)

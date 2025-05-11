from slt_runner.base_sql_statement import BaseSqlStatement
from slt_runner.my_token import Token
from typing import Optional
from enum import Enum


class SortStyle(Enum):
    NO_SORT = 0
    ROW_SORT = 1
    VALUE_SORT = 2
    UNKNOWN = 3


class Query(BaseSqlStatement):
    def __init__(self, header: Token, line: int):
        super().__init__(header, line)
        self.label: Optional[str] = None
        self.connection_name: Optional[str] = None
        self.sortstyle: Optional[SortStyle] = None
        self.label: Optional[str] = None

    def set_connection(self, connection: str):
        self.connection_name = connection

    def set_sortstyle(self, sortstyle: SortStyle):
        self.sortstyle = sortstyle

    def get_sortstyle(self) -> Optional[SortStyle]:
        return self.sortstyle

    def set_label(self, label: str):
        self.label = label

    def get_label(self) -> Optional[str]:
        return self.label

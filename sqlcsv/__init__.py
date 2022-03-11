import sys
import sqlite3

from typing import Union, Optional
from pathlib import Path
from collections import namedtuple

_MODULE_PATH = Path(__file__).parent
_CSV_EXT_PATH = _MODULE_PATH / 'ext/csv'


class SQLiteProc:

    SCHEMA = 'temp'

    def __init__(self):
        self._con = sqlite3.connect(':memory:')

        if sys.version_info >= (3, 10):
            sqlite3.enable_load_extension(True)
            sqlite3.load_extension(str(_CSV_EXT_PATH))
            sqlite3.enable_load_extension(False)
        else:
            self._con.enable_load_extension(True)
            self._con.load_extension(str(_CSV_EXT_PATH))
            self._con.enable_load_extension(False)

        self.cursor = self._con.cursor()
    
    def exec(self, query: str):
        return self.cursor.execute(query)

    @classmethod
    def table(cls, table_name: str) -> str:
        return f'{cls.SCHEMA}.{table_name}'

    @classmethod
    def prepare_query(cls, query):
        tokens = query.split()

        for i, token in enumerate(tokens):
            if token.lower() == 'from':
                table_token_index = i + 1
                tokens[table_token_index] = cls.table(tokens[table_token_index])
                break
        
        return ' '.join(tokens)

    def __del__(self):
        self._con.close()


_proc = SQLiteProc()


def execute(query: str) -> list:
    return _proc.exec(_proc.prepare_query(query)).fetchall()


class Table:

    def __init__(self, name: str, columns: list[str]):
        self.name = name
        self.columns = columns

    def execute(self, query: str) -> list[namedtuple]:
        cursor = _proc.exec(_proc.prepare_query(query))
        columns = [col_name for col_name, _, _, _, _, _, _ in cursor.description]

        row_fabric = namedtuple('Row', columns)

        return [row_fabric(*data) for data in cursor]

    def __str__(self):
        return f'Table {self.name}'


def read(fp: Union[str, Path], table_name: Optional[str]=None, header: bool=True) -> str:
    if isinstance(fp, str):
        fp = Path(fp)

    if table_name is None:
        table_name = fp.name.removesuffix(fp.suffix)

    header_str = str(header).lower()

    create_query = (
        f"CREATE VIRTUAL TABLE {_proc.table(table_name)} "
        f"USING csv(filename='{str(fp)}', header={header_str});"
    )
    table_info_query = f"PRAGMA {_proc.SCHEMA}.table_info('{table_name}')"

    _proc.exec(create_query)

    columns = [
        col_name
        for _, col_name, _, _, _, _ in _proc.exec(table_info_query)
    ]

    return Table(table_name, columns)

import json
import sys
import typing as t
from enum import auto

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum

from attrs import define


@define(frozen=True)
class TableAddress:
    schema: str
    table: str

    @property
    def fqn(self):
        if not self.schema:
            raise ValueError("Unable to compute a full-qualified table name without schema name")
        return f"{self.quote_identifier(self.schema)}.{self.quote_identifier(self.table)}"

    @staticmethod
    def quote_identifier(name: str) -> str:
        """
        Poor man's table quoting.

        TODO: Better use or vendorize canonical table quoting function from CrateDB Toolkit, when applicable.
        """
        if name and '"' not in name:
            name = f'"{name}"'
        return name


class ColumnType(StrEnum):
    MAP = auto()


@define(frozen=True)
class ColumnTypeMap:
    column: str
    type: ColumnType


class PrimaryKeyStore(dict):
    pass


class ColumnTypeMapStore(dict):
    def add(self, table: TableAddress, column: str, type_: ColumnType):
        self.setdefault(table, {})
        self[table][column] = type_
        return self

    def to_dict(self) -> dict:
        data = {}
        for key, value in self.items():
            tbl = f"{key.schema}:{key.table}"
            for column, type_ in value.items():
                key = f"{tbl}:{column}"
                data[key] = type_.value
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> t.Union["ColumnTypeMapStore", None]:
        if not data:
            return None
        ctms = cls()
        for key, type_ in data.items():
            schema, table, column = key.split(":")
            ctms.add(TableAddress(schema=schema, table=table), column=column, type_=ColumnType(type_))
        return ctms

    @classmethod
    def from_json(cls, payload: str) -> t.Union["ColumnTypeMapStore", None]:
        if not payload:
            return None
        return cls.from_dict(json.loads(payload))

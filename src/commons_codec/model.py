import json
import sys
import typing as t
from enum import Enum, auto
from functools import cached_property

import toolz
from attr import Factory
from attrs import define
from sqlalchemy_cratedb.support import quote_relation_name

from commons_codec.util.data import TaggableList

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # pragma: no cover


@define(frozen=True)
class TableAddress:
    schema: str
    table: str

    @cached_property
    def fqn(self):
        if not self.schema:
            raise ValueError("Unable to compute a full-qualified table name without schema name")
        return quote_relation_name(f"{self.schema}.{self.table}")


class ColumnType(StrEnum):
    MAP = auto()
    OBJECT = auto()


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


@define
class SQLOperation:
    """
    Bundle data about an SQL operation, including statement and parameters.

    Parameters can be a single dictionary or a list of dictionaries.
    """

    statement: str
    parameters: t.Optional[t.Union[t.Mapping[str, t.Any], t.List[t.Mapping[str, t.Any]]]] = None


class SkipOperation(Exception):
    pass


@define
class SQLParameterizedClause:
    """
    Manage details about a SQL parameterized clause, including column names, parameter names, and values.
    """

    lvals: t.List[str] = Factory(list)
    rvals: t.List[str] = Factory(list)
    values: t.Dict[str, t.Any] = Factory(dict)

    def add(self, lval: str, value: t.Any, name: str, rval: str = None):
        self.lvals.append(lval)
        if rval is None:
            self.rvals.append(f":{name}")
        else:
            self.rvals.append(rval)
        self.values[name] = value

    def render(self, delimiter: str) -> str:
        """
        Render a clause of an SQL statement.
        """
        return delimiter.join([f"{lval}={rval}" for lval, rval in zip(self.lvals, self.rvals)])

    def render_lvals(self):
        return ", ".join([f'"{item}"' for item in self.lvals])

    def render_rvals(self):
        return ", ".join(self.rvals)


@define
class SQLParameterizedSetClause(SQLParameterizedClause):
    def to_sql(self):
        """
        Render a SET clause of an SQL statement.
        """
        return self.render(", ")


@define
class SQLParameterizedWhereClause(SQLParameterizedClause):
    def to_sql(self):
        """
        Render a WHERE clause of an SQL statement.
        """
        return self.render(" AND ")


@define
class UniversalRecord:
    """
    Manage a universal record including primary keys and two halves of a record.
    One bucket stores the typed fields, the other stores the untyped ones.
    """

    pk: t.Dict[str, t.Any]
    typed: t.Dict[str, t.Any]
    untyped: t.Dict[str, t.Any]

    def to_dict(self):
        return {"pk": self.pk, "typed": self.typed, "untyped": self.untyped}

    @classmethod
    def from_record(
        cls, record: t.Dict[str, t.Any], primary_keys: t.Union[t.List[str], None] = None
    ) -> "UniversalRecord":
        pk = {}
        untyped = {}
        primary_keys_effective = set(primary_keys or [])
        for key, value in record.items():
            if key in primary_keys_effective:
                pk[key] = value
            if isinstance(value, TaggableList) and value.get_tag("varied", False):
                untyped[key] = value
        record = toolz.dissoc(record, *pk.keys())
        record = toolz.dissoc(record, *untyped.keys())
        return cls(pk=pk, typed=record, untyped=untyped)


class ColumnMappingStrategy(Enum):
    DIRECT = "DIRECT"
    UNIVERSAL = "UNIVERSAL"

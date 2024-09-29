import sys
import typing as t

from attr import Factory, define

if sys.version_info >= (3, 11):
    from enum import StrEnum
else:
    from backports.strenum import StrEnum  # pragma: no cover


class AttributeType(StrEnum):
    STRING = "STRING"
    NUMBER = "NUMBER"
    BINARY = "BINARY"


DYNAMODB_TYPE_MAP = {
    "S": AttributeType.STRING,
    "N": AttributeType.NUMBER,
    "B": AttributeType.BINARY,
}

CRATEDB_TYPE_MAP = {
    AttributeType.STRING: "STRING",
    AttributeType.NUMBER: "BIGINT",
    AttributeType.BINARY: "STRING",
}


@define
class Attribute:
    name: str
    type: AttributeType

    @classmethod
    def from_dynamodb(cls, name: str, type_: str):
        try:
            return cls(name=name, type=DYNAMODB_TYPE_MAP[type_])
        except KeyError as ex:
            raise KeyError(f"Mapping DynamoDB type failed: name={name}, type={type_}") from ex

    @property
    def cratedb_type(self):
        return CRATEDB_TYPE_MAP[self.type]


@define
class PrimaryKeySchema:
    schema: t.List[Attribute] = Factory(list)

    def add(self, name: str, type: str) -> "PrimaryKeySchema":  # noqa: A002
        self.schema.append(Attribute.from_dynamodb(name, type))
        return self

    @classmethod
    def from_table(cls, table) -> "PrimaryKeySchema":
        """
        # attribute_definitions: [{'AttributeName': 'Id', 'AttributeType': 'N'}]
        # key_schema: [{'AttributeName': 'Id', 'KeyType': 'HASH'}]
        """

        schema = cls()
        attribute_type_map: t.Dict[str, str] = {}
        for attr in table.attribute_definitions:
            attribute_type_map[attr["AttributeName"]] = attr["AttributeType"]

        for key in table.key_schema:
            name = key["AttributeName"]
            type_ = attribute_type_map[name]
            schema.add(name=name, type=type_)

        return schema

    def keys(self) -> t.List[str]:
        return [attribute.name for attribute in self.schema]

    def column_names(self) -> t.List[str]:
        return [f'"{attribute.name}"' for attribute in self.schema]

    def to_sql_ddl_clauses(self) -> t.List[str]:
        return [f'"{attribute.name}" {attribute.cratedb_type} PRIMARY KEY' for attribute in self.schema]

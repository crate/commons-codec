import typing as t
from collections import OrderedDict

import attr
from attr import Factory
from attrs import define
from cattrs.preconf.json import make_converter as make_json_converter
from cattrs.preconf.pyyaml import make_converter as make_yaml_converter

from zyp.util.data import no_privates_no_nulls_no_empties


@define
class Metadata:
    version: t.Union[int, None] = None
    type: t.Union[str, None] = None


@define
class SchemaDefinitionRule:
    pointer: str
    type: str


@define
class SchemaDefinition:
    rules: t.List[SchemaDefinitionRule] = Factory(list)
    _map: t.Dict[str, str] = Factory(dict)

    def add(self, pointer: str, type: str) -> "SchemaDefinition":  # noqa: A002
        return self._add(SchemaDefinitionRule(pointer=pointer, type=type))

    def __attrs_post_init__(self):
        if self.rules and not self._map:
            for rule in self.rules:
                self._add_runtime(rule)

    def _add(self, rule: SchemaDefinitionRule) -> "SchemaDefinition":
        self.rules.append(rule)
        self._add_runtime(rule)
        return self

    def _add_runtime(self, rule: SchemaDefinitionRule) -> "SchemaDefinition":
        self._map[rule.pointer] = rule.type
        return self


@define
class Dumpable:
    meta: t.Union[Metadata, None] = None

    def to_dict(self) -> t.Dict[str, t.Any]:
        return attr.asdict(self, dict_factory=OrderedDict, filter=no_privates_no_nulls_no_empties)

    def to_json(self) -> str:
        converter = make_json_converter(dict_factory=OrderedDict)
        return converter.dumps(self.to_dict())

    def to_yaml(self) -> str:
        converter = make_yaml_converter(dict_factory=OrderedDict)
        return converter.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: t.Dict[str, t.Any]):
        return cls(**data)

    @classmethod
    def from_json(cls, json_str: str):
        converter = make_json_converter(dict_factory=OrderedDict)
        return converter.loads(json_str, cls)

    @classmethod
    def from_yaml(cls, yaml_str: str):
        converter = make_yaml_converter(dict_factory=OrderedDict)
        return converter.loads(yaml_str, cls)

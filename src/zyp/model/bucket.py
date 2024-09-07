import importlib
import logging
import typing as t

import jmespath
import jq
import jsonpointer
import transon
from attr import Factory
from attrs import define
from jsonpointer import JsonPointer

from zyp.model.base import Dumpable, Metadata, SchemaDefinition
from zyp.util.dictx import OrderedDictX
from zyp.util.locator import swap_node, to_pointer

logger = logging.getLogger(__name__)


TransonTemplate = t.Dict[str, t.Any]
MokshaTransformer = t.Union[jmespath.parser.ParsedResult, jq._Program, transon.Transformer]


@define
class ConverterRuleBase:
    def compile(self):
        raise NotImplementedError("Please implement this method")


@define
class ConverterBase:
    rules: t.List[t.Any] = Factory(list)
    _runtime_rules: t.List[t.Any] = Factory(list)

    def __attrs_post_init__(self):
        if self.rules and not self._runtime_rules:
            for rule in self.rules:
                self._add_runtime(rule)

    def _add_rule(self, rule):
        self.rules.append(rule)
        self._add_runtime(rule)
        return self

    def _add_runtime(self, rule):
        self._runtime_rules.append(rule.compile())
        return self


@define
class ValueConverterRule(ConverterRuleBase):
    pointer: str
    transformer: str
    args: t.Union[t.List[t.Any], None] = Factory(list)

    def compile(self):
        pointer = to_pointer(self.pointer)
        if isinstance(self.transformer, str):
            if not self.transformer:
                raise ValueError("Empty transformer reference")
            transformer_function = self._resolve_fun(self.transformer)
        else:
            transformer_function = self.transformer
        if self.args:
            transformer_function = transformer_function(*self.args)
        return ValueConverterRuntimeRule(pointer=pointer, transformer=transformer_function)

    @staticmethod
    def _resolve_fun(symbol: str) -> t.Callable:
        if "." not in symbol:
            symbol = f"zyp.function.{symbol}"
        modname, symbol = symbol.rsplit(".", 1)
        mod = importlib.import_module(modname)
        return getattr(mod, symbol)


@define
class ValueConverterRuntimeRule:
    pointer: jsonpointer.JsonPointer
    transformer: t.Callable


@define
class ValueConverter(ConverterBase):
    rules: t.List[ValueConverterRule] = Factory(list)
    _runtime_rules: t.List[ValueConverterRuntimeRule] = Factory(list)

    def add(self, pointer: str, transformer: str, args: t.List[t.Any] = None) -> "ValueConverter":
        self._add_rule(ValueConverterRule(pointer=pointer, transformer=transformer, args=args))
        return self

    def apply(self, data: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for rule in self._runtime_rules:
            data = t.cast(t.Dict[str, t.Any], swap_node(rule.pointer, data, rule.transformer))
        return data


@define
class FieldRenamerRule:
    old: str
    new: str


@define
class FieldRenamer:
    rules: t.List[FieldRenamerRule] = Factory(list)

    def add(self, old: str, new: str) -> "FieldRenamer":
        self.rules.append(FieldRenamerRule(old=old, new=new))
        return self

    def apply(self, data: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        d = OrderedDictX(data)
        for rule in self.rules:
            d.rename_key(rule.old, rule.new)
        return d


@define
class TransonRule:
    pointer: str
    template: TransonTemplate

    def compile(self):
        return TransonRuntimeRule(to_pointer(self.pointer), transformer=transon.Transformer(self.template))


@define
class TransonRuntimeRule:
    pointer: JsonPointer
    transformer: transon.Transformer


@define
class TransonTransformation(ConverterBase):
    rules: t.List[TransonRule] = Factory(list)
    _runtime_rules: t.List[TransonRuntimeRule] = Factory(list)

    def add(self, pointer: str, template: TransonTemplate) -> "TransonTransformation":
        self._add_rule(TransonRule(pointer=pointer, template=template))
        return self

    def apply(self, data: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        for rule in self._runtime_rules:
            data = t.cast(t.Dict[str, t.Any], swap_node(rule.pointer, data, rule.transformer.transform))
        return data


@define
class BucketTransformation(Dumpable):
    """
    A minimal transformation engine.

    Based on:
    - JSON Pointer (RFC 6901)
    - Transon

    Documentation:
    - https://www.rfc-editor.org/rfc/rfc6901
    - https://transon-org.github.io/
    """

    meta: Metadata = Metadata(version=1, type="zyp-bucket")
    schema: t.Union[SchemaDefinition, None] = None
    names: t.Union[FieldRenamer, None] = None
    values: t.Union[ValueConverter, None] = None
    transon: t.Union[TransonTransformation, None] = None

    def apply(self, data: t.Dict[str, t.Any]) -> t.Dict[str, t.Any]:
        if self.names:
            data = self.names.apply(data)
        if self.values:
            data = self.values.apply(data)
        if self.transon:
            data = self.transon.apply(data)
        return data

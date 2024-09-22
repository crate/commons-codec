import collections
import typing as t

import jmespath
import jq
import transon
from attr import Factory
from attrs import define

from zyp.model.base import DictOrList
from zyp.model.bucket import ConverterBase, MokshaTransformer, TransonTemplate
from zyp.util.expression import compile_expression


@define
class MokshaRule:
    type: str
    expression: t.Union[str, TransonTemplate]
    disabled: t.Optional[bool] = False

    def compile(self):
        return MokshaRuntimeRule(self.type, compile_expression(self.type, self.expression), disabled=self.disabled)


@define
class MokshaRuntimeRule:
    type: str
    transformer: MokshaTransformer
    disabled: t.Optional[bool] = False

    def evaluate(self, data: DictOrList) -> DictOrList:
        if self.disabled:
            return data
        if isinstance(self.transformer, jmespath.parser.ParsedResult):
            return self.transformer.search(data, options=jmespath.Options(dict_cls=collections.OrderedDict))
        elif isinstance(self.transformer, jq._Program):
            if isinstance(data, map):
                data = list(data)
            return self.transformer.transform(data)
        elif isinstance(self.transformer, transon.Transformer):
            return self.transformer.transform(data)
        else:
            raise TypeError(f"Evaluation failed. Type must be either jmes or jq or transon: {self.transformer}")


@define
class MokshaTransformation(ConverterBase):
    rules: t.List[MokshaRule] = Factory(list)
    _runtime_rules: t.List[MokshaRuntimeRule] = Factory(list)

    def jmes(self, expression: str) -> "MokshaTransformation":
        if not expression:
            raise ValueError("JMESPath expression cannot be empty")

        self._add_rule(MokshaRule(type="jmes", expression=expression))
        return self

    def jq(self, expression: str) -> "MokshaTransformation":
        if not expression:
            raise ValueError("jq expression cannot be empty")

        self._add_rule(MokshaRule(type="jq", expression=expression))
        return self

    def transon(self, expression: TransonTemplate) -> "MokshaTransformation":
        if not expression:
            raise ValueError("transon expression cannot be empty")

        self._add_rule(MokshaRule(type="transon", expression=expression))
        return self

    def apply(self, data: DictOrList) -> DictOrList:
        for rule in self._runtime_rules:
            data = rule.evaluate(data)
        return data

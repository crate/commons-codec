import collections
import typing as t

import jmespath
import jq
import transon
from attr import Factory
from attrs import define

from zyp.model.bucket import ConverterBase, DictOrList, MokshaTransformer, TransonTemplate
from zyp.util.expression import compile_expression


@define
class MokshaRule:
    type: str
    expression: t.Union[str, TransonTemplate]

    def compile(self):
        return MokshaRuntimeRule(self.type, compile_expression(self.type, self.expression))


@define
class MokshaRuntimeRule:
    type: str
    transformer: MokshaTransformer

    def evaluate(self, data: DictOrList) -> DictOrList:
        if isinstance(self.transformer, jmespath.parser.ParsedResult):
            return self.transformer.search(data, options=jmespath.Options(dict_cls=collections.OrderedDict))
        elif isinstance(self.transformer, jq._Program):
            return self.transformer.input_value(data).first()
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

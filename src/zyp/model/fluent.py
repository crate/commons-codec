import typing as t

from attrs import define

from zyp.model.bucket import ConverterBase
from zyp.model.moksha import MokshaRule


@define
class FluentTransformation(ConverterBase):
    rules = t.List[t.Any]

    def jmes(self, expression) -> "FluentTransformation":
        self._add_rule(MokshaRule(type="jmes", expression=expression))
        return self

    def jq(self, expression) -> "FluentTransformation":
        self._add_rule(MokshaRule(type="jq", expression=expression))
        return self

    def rename_fields(self, definition: t.Dict[str, t.Any]) -> "FluentTransformation":
        return self

    def convert_values(self, definition: t.Dict[str, t.Any], type: str) -> "FluentTransformation":  # noqa: A002
        return self

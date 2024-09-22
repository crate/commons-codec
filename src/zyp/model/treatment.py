import builtins
import typing as t

from attr import Factory
from attrs import define

from zyp.model.base import Collection, DictOrList, Dumpable, Record


@define
class Treatment(Dumpable):
    ignore_complex_lists: bool = False
    ignore_field: t.List[str] = Factory(list)
    convert_list: t.List[str] = Factory(list)
    convert_string: t.List[str] = Factory(list)
    convert_dict: t.List[t.Dict[str, str]] = Factory(list)
    normalize_complex_lists: bool = False
    prune_invalid_date: t.List[str] = Factory(list)

    def apply(self, data: DictOrList) -> DictOrList:
        if isinstance(data, dict):
            self.apply_record(data)
            return {k: self.apply(v) for (k, v) in data.items()}
        elif isinstance(data, list):
            return t.cast(list, [self.apply(v) for v in data])
        return data

    def apply_record(self, data: Record) -> Record:
        # Optionally ignore lists of complex objects.
        local_ignores = []
        if self.ignore_complex_lists:
            for k, v in data.items():
                if self.is_list_of_dicts(v):
                    # Never ignore items in MongoDB Extended JSON format.
                    if v[0] and next(iter(v[0])).startswith("$"):
                        continue
                    local_ignores.append(k)

        # Apply global and computed ignores.
        for ignore_name in self.ignore_field + local_ignores:
            if ignore_name in data:
                del data[ignore_name]

        # Apply normalization for lists of objects.
        if self.normalize_complex_lists:
            for _, v in data.items():
                if self.is_list_of_dicts(v):
                    ListOfVaryingObjectsNormalizer(v).apply()

        # Converge certain items to `list` even when defined differently.
        for to_list_name in self.convert_list:
            if to_list_name in data and not isinstance(data[to_list_name], list):
                data[to_list_name] = [data[to_list_name]]

        # Converge certain items to `str` even when defined differently.
        for name in self.convert_string:
            if name in data and not isinstance(data[name], str):
                data[name] = str(data[name])

        # Converge certain items to `dict` even when defined differently.
        for rule in self.convert_dict:
            name = rule["name"]
            wrapper_name = rule["wrapper_name"]
            if name in data and not isinstance(data[name], dict):
                data[name] = {wrapper_name: data[name]}

        # Prune invalid date representations.
        for key in self.prune_invalid_date:
            if key in data:
                if not isinstance(data[key], dict):
                    del data[key]
                elif "date" in data[key]:
                    if isinstance(data[key]["date"], str):
                        del data[key]

        return data

    @staticmethod
    def is_list_of_dicts(v: t.Any) -> bool:
        return isinstance(v, list) and bool(v) and isinstance(v[0], dict)


@define
class NormalizerRule:
    """
    Manage details of a normalizer rule.
    """

    name: str
    converter: t.Callable


@define
class ListOfVaryingObjectsNormalizer:
    """
    CrateDB can not store lists of varying objects, so try to normalize them.
    """

    data: Collection

    def apply(self):
        self.apply_rules(self.get_rules(self.type_stats()))

    def apply_rules(self, rules: t.List[NormalizerRule]) -> None:
        for item in self.data:
            for rule in rules:
                name = rule.name
                if name in item:
                    item[name] = rule.converter(item[name])

    def get_rules(self, statistics) -> t.List[NormalizerRule]:
        rules = []
        for name, types in statistics.items():
            if len(types) > 1:
                rules.append(NormalizerRule(name=name, converter=self.get_best_converter(types)))
        return rules

    def type_stats(self) -> t.Dict[str, t.List[str]]:
        types: t.Dict[str, t.List[str]] = {}
        for item in self.data:
            for key, value in item.items():
                types.setdefault(key, []).append(type(value).__name__)
        return types

    @staticmethod
    def get_best_converter(types: t.List[str]) -> t.Callable:
        if "str" in types:
            return builtins.str
        if "float" in types and "int" in types and "str" not in types:
            return builtins.float
        return lambda x: x

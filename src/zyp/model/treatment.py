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
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    # Skip ignoring special-encoded items.
                    if v[0] and list(v[0].keys())[0].startswith("$"):
                        continue
                    local_ignores.append(k)

        # Apply global and computed ignores.
        for ignore_name in self.ignore_field + local_ignores:
            if ignore_name in data:
                del data[ignore_name]

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

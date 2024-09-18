from copy import deepcopy
from pathlib import Path

import yaml

from zyp.model.bucket import BucketTransformation, FieldRenamer, ValueConverter
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation


class CompositeRecipe:
    """
    Composite recipe composed of demo input and output data and a corresponding transformation.

    It executes the following steps, in order of appearance:

    - Unwrap `records` attribute from container dictionary into actual collection.
    - Filter collection, both by omitting invalid/empty records, and by applying query constrains.
    - On each record, rename the top-level `_id` field to `id`.
    - On each record, apply value conversions to two nested data values.
    - Postprocess collection, applying a custom value scaling factor.
    """

    # Define a messy input data collection.
    data_in = {
        "message-source": "system-3000",
        "message-type": "eai-warehouse",
        "records": [
            {"_id": "12", "meta": {"name": "foo", "location": "B"}, "data": {"value": "4242"}},
            None,
            {"_id": "34", "meta": {"name": "bar", "location": "BY"}, "data": {"value": -8401}},
            {"_id": "56", "meta": {"name": "baz", "location": "NI"}, "data": {"value": 2323}},
            {"_id": "78", "meta": {"name": "qux", "location": "NRW"}, "data": {"value": -580}},
            None,
            None,
        ],
    }

    # Define expectation of the cleansed data collection.
    data_out = [
        {"id": 12, "meta": {"name": "foo", "location": "B"}, "data": {"value": 42.42}},
        {"id": 34, "meta": {"name": "bar", "location": "BY"}, "data": {"value": -84.01}},
    ]

    # Define transformation.
    transformation = CollectionTransformation(
        pre=MokshaTransformation().jmes("records[?not_null(meta.location) && !starts_with(meta.location, 'N')]"),
        bucket=BucketTransformation(
            names=FieldRenamer().add(old="_id", new="id"),
            values=ValueConverter()
            .add(pointer="/id", transformer="builtins.int")
            .add(pointer="/data/value", transformer="builtins.float"),
        ),
        post=MokshaTransformation().jq(".[] |= (.data.value /= 100)"),
    )


class ListRecipes:
    """
    Demo transformation recipe for handling anomalies in lists.
    """

    # Define a messy input data collection.
    data_in = {
        "message-source": "community",
        "message-type": "mixed-pickles",
        "records": [
            {
                "data": {
                    "to-list-flat": [{"foo": 1}, [{"foo": 2}, {"foo": 3}]],
                },
            },
        ],
    }

    # Define expectation of the cleansed data collection.
    data_out = [
        {
            "data": {
                "to-list-flat": [{"foo": 1}, {"foo": 2}, {"foo": 3}],
            },
        },
    ]

    # Define transformation.
    transformation = CollectionTransformation(
        pre=MokshaTransformation().jq(".records").jq('.[] |= (.data."to-list-flat" |= flatten)'),
    )


def test_collection_transformation_composite_success():
    """
    Verify transformation recipe for re-shaping a collection of records.
    """
    assert CompositeRecipe.transformation.apply(CompositeRecipe.data_in) == CompositeRecipe.data_out


def test_collection_transformation_lists_success():
    """
    Verify transformation recipe for re-shaping anomalies in lists.
    """
    assert ListRecipes.transformation.apply(ListRecipes.data_in) == ListRecipes.data_out


def test_collection_transformation_serialize():
    """
    Verify collection transformation description can be serialized to a data structure and back.
    """
    transformation = CompositeRecipe.transformation
    transformation_dict = {
        "meta": {"version": 1, "type": "zyp-collection"},
        "pre": {
            "rules": [
                {"type": "jmes", "expression": "records[?not_null(meta.location) && !starts_with(meta.location, 'N')]"}
            ]
        },
        "bucket": {
            "meta": {"version": 1, "type": "zyp-bucket"},
            "names": {"rules": [{"old": "_id", "new": "id"}]},
            "values": {
                "rules": [
                    {"pointer": "/id", "transformer": "builtins.int"},
                    {"pointer": "/data/value", "transformer": "builtins.float"},
                ]
            },
        },
        "post": {"rules": [{"type": "jq", "expression": ".[] |= (.data.value /= 100)"}]},
    }
    dict_result = transformation.to_dict()
    assert dict_result == transformation_dict

    yaml_result = transformation.to_yaml()
    assert yaml.full_load(yaml_result) == transformation_dict
    transformation_second = CollectionTransformation.from_yaml(yaml_result)
    assert isinstance(transformation_second, CollectionTransformation)


def test_collection_transformation_regular_load_and_apply():
    """
    Verify rule-based transformations can be loaded and applied.
    """
    payload = Path("tests/zyp/transformation-collection.yaml").read_text()
    transformation = CollectionTransformation.from_yaml(payload)
    result = transformation.apply(deepcopy(CompositeRecipe.data_in))
    assert result == CompositeRecipe.data_out


def test_collection_transformation_treatment_load_and_apply():
    """
    Verify collection transformation with treatment can be loaded and applied.
    """
    payload = Path("tests/zyp/transformation-collection-treatment.yaml").read_text()
    transformation = CollectionTransformation.from_yaml(payload)
    result = transformation.apply(deepcopy(CompositeRecipe.data_in))
    assert result == {
        "message-source": "system-3000",
        "message-type": "eai-warehouse",
    }

from zyp.model.fluent import FluentTransformation


def test_fluent_transformation():
    """
    FIXME: Fluent transformations are not implemented yet.
    """
    transformation = (
        FluentTransformation()
        .jmes("records[?starts_with(location, 'B')]")
        .rename_fields({"_id": "id"})
        .convert_values({"/id": "int", "/value": "float"}, type="pointer-python")
        .jq(".[] |= (.value /= 100)")
    )
    assert len(transformation.rules) == 2

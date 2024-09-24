# Zyp Backlog

## Iteration +1
- [ ] Documentation: jqlang stdlib's `to_object` function for substructure management
- [ ] Documentation: Type casting
  `echo '{"a": 42, "b": {}, "c": []}' | jq -c '.|= (.b |= objects | .c |= objects)'`
  `{"a":42,"b":{}}`
- [ ] Renaming currently needs JSON Pointer support, implemented in Python.
  Alternatively, can `jq` also do it?
- [ ] Simple IFTTT: When condition, do that (i.e. add tag)
- [ ] Documentation: `jq` functions
  - `builtin.jq`: https://github.com/jqlang/jq/blob/master/src/builtin.jq
  - `function.jq`
- [ ] Documentation: Update "What’s Inside"
- [ ] Documentation: Usage (build (API, from_yaml), apply)
- [ ] Documentation: How to extend `function.{jq,py}`

### Documentation
```
- Omit records `and .value.bill_contact.id != ""`
# Only accept `email` elements that are objects.
#and (if (.value | index("emails")) then (.value.emails[].type | type) == "object" else true end)

# Exclude a few specific documents.
# TODO: Review documents once more to discover more edge cases.
.[] |= select(
and ._id != "55d71c8ce4b02210dc47b10f"
)

# Some early `phone` elements have been stored wrongly,
# all others are of type OBJECT.
#and (.value.phone | type) != "array"

# Some early `urls` elements have been stored wrongly,
# all others are of type ARRAY.
#and (.value.urls | type) != "object"
```


## Iteration +2
- [ ] CLI interface
- [ ] Documentation: Add Python example to "Synopsis" section on /index.html
- [ ] Documentation: Compare with Seatunnel
  https://github.com/apache/seatunnel/tree/dev/docs/en/transform-v2

Demonstrate more use cases, like...
- [ ] math expressions
- [ ] omit key (recursively)
- [ ] combine keys
- [ ] filter on keys and/or values
- [ ] Pathological cases like "Not defined" in typed fields like `TIMESTAMP`
- [ ] Use simpleeval, like Meltano, and provide the same built-in functions
  - https://sdk.meltano.com/en/v0.39.1/stream_maps.html#other-built-in-functions-and-names
  - https://github.com/MeltanoLabs/meltano-map-transform/pull/255
  - https://github.com/MeltanoLabs/meltano-map-transform/issues/252
- [ ] Use JSONPath, see https://sdk.meltano.com/en/v0.39.1/code_samples.html#use-a-jsonpath-expression-to-extract-the-next-page-url-from-a-hateoas-response

## Iteration +3
- [ ] Moksha transformations on Buckets
- [ ] Fluent API interface
  ```python
  from zyp.model.fluent import FluentTransformation

  transformation = FluentTransformation()
  .jmes("records[?starts_with(location, 'B')]")
  .rename_fields({"_id": "id"})
  .convert_values({"/id": "int", "/value": "float"}, type="pointer-python")
  .jq(".[] |= (.value /= 100)")
  ```
- [ ] Investigate using JSON Schema
- [ ] https://github.com/Halvani/alphabetic
- [ ] Mappers do not support external API lookups.
  To add external API lookups, you can either (a) land all your data and
  then joins using a transformation tool like dbt, or (b) create a custom
  mapper plugin with inline lookup logic.
  => Example from Luftdatenpumpe, using a reverse geocoder
- [ ] Define schema
  - https://sdk.meltano.com/en/latest/typing.html
  - https://docs.meltano.com/guide/v2-migration/#migrate-to-an-adapter-specific-dbt-transformer
  - https://github.com/meltano/sdk/blob/v0.39.1/singer_sdk/mapper.py
- [ ] Is `jqpy` better than `jq`?
  - https://baterflyrity.github.io/jqpy/
- [ ] Load XML via Badgerfish or KDL
  https://github.com/kdl-org/kdl

## Done
- [x] Refactor module namespace to `zyp`
- [x] Documentation
- [x] Apply to MongoDB Table Loader in CrateDB Toolkit
- [x] Model: Toggle rule active / inactive by respecting `disabled` flag
- [x] Documentation: How to delete attributes from lists using jq?
- [x] Review and test jqlang stdlib's `to_object` function

# Zyp Backlog

## Iteration +1
- [x] Refactor module namespace to `zyp`
- [x] Documentation
- [ ] CLI interface
- [x] Apply to MongoDB Table Loader in CrateDB Toolkit
- [ ] Document `jq` functions
  - `builtin.jq`: https://github.com/jqlang/jq/blob/master/src/builtin.jq
  - `function.jq`
- [ ] Renaming needs JSON Pointer support. Alternatively, can `jq` do it?
- [ ] Documentation: Add Python example to "Synopsis" section on /index.html

## Iteration +2
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

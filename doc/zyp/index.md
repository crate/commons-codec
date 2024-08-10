# Zyp Transformations

## About
A data model and implementation for a compact transformation engine written
in [Python], based on [JSON Pointer] (RFC 6901), [JMESPath], and [transon],
implemented using [attrs] and [cattrs].

## Ideas
:Conciseness:
    Define a multistep data refinement process with as little code as possible.
:Low Footprint:
    Doesn't need any infrastructure or pipeline framework. It's just a little library.
:Interoperability:
    Marshal transformation recipe definition to/from text-only representations (JSON,
    YAML), in order to encourage implementations in other languages.
:Performance:
    Well, it is written in Python. Fragments can be re-written in Rust, when applicable.
:Immediate:
    Other ETL frameworks and concepts often need to first land your data in the target
    system before applying subsequent transformations. Zyp is working directly within
    the data pipeline, before data is inserted into the target system.

## Synopsis I
A basic transformation example for individual data records.

```python
from zyp.model.bucket import BucketTransformation, FieldRenamer, ValueConverter

# Consider a slightly messy collection of records.
data_in = [
    {"_id": "123", "name": "device-foo", "reading": "42.42"},
    {"_id": "456", "name": "device-bar", "reading": -84.01},
]

# Define a transformation that renames the `_id` field to `id`,
# casts its value to `int`, and casts the `reading` field to `float`. 
transformation = BucketTransformation(
    names=FieldRenamer().add(old="_id", new="id"),
    values=ValueConverter()
    .add(pointer="/id", transformer="builtins.int")
    .add(pointer="/reading", transformer="builtins.float"),
)

for record in data_in:
    print(transformation.apply(record))
```
The result is a transformed data collection.
```json
[
  {"id": 123, "name": "device-foo", "reading": 42.42},
  {"id": 456, "name": "device-bar", "reading": -84.01}
]
```

## Synopsis II
A more advanced transformation example for a collection of data records.

Consider a messy collection of input data.
- The actual collection is nested within the top-level `records` item.
- `_id` fields are conveyed in string format.
- `value` fields include both integer and string values.
- `value` fields are fixed-point values, using a scaling factor of `100`.
- The collection includes invalid `null` records.
  Those records usually trip processing when, for example, filtering on object items.
```python
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
```

Consider after applying a corresponding transformation, the expected outcome is a
collection of valid records, optionally filtered, and values adjusted according
to relevant type hints and other conversions.
```python
data_out = [
  {"id": 12, "meta": {"name": "foo", "location": "B"}, "data": {"value": 42.42}},
  {"id": 34, "meta": {"name": "bar", "location": "BY"}, "data": {"value": -84.01}},
]
```

Let's come up with relevant pre-processing rules to cleanse and mangle the shape of the
input collection. In order to make this example more exciting, let's include two special
needs:
- Filter input collection by value of nested element.
- Rename top-level fields starting with underscore `_`.

Other than those special rules, the fundamental ones to re-shape the data are:
- Unwrap `records` attribute from container dictionary into actual collection.
- Filter collection, both by omitting invalid/empty records, and by applying query
  constrains.
- On each record, rename the top-level `_id` field to `id`.
- On each record, adjust the data types of the `id` and `value` fields.
- Postprocess collection, applying a custom scaling factor to the `value` field.

Zyp let's you concisely write those rules down, using the Python language.

```python
from zyp.model.bucket import BucketTransformation, FieldRenamer, ValueConverter
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation

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

data_out = transformation.apply(data_in)
```
Alternatively, serialize the `zyp-collection` transformation description,
for example into YAML format.
```python
print(transformation.to_yaml())
```
```yaml
meta:
  version: 1
  type: zyp-collection
pre:
  rules:
  - expression: records[?not_null(meta.location) && !starts_with(meta.location, 'N')]
    type: jmes
bucket:
  names:
    rules:
    - new: id
      old: _id
  values:
    rules:
    - pointer: /id
      transformer: builtins.int
    - pointer: /data/value
      transformer: builtins.float
post:
  rules:
  - expression: .[] |= (.data.value /= 100)
    type: jq
```


## Prior Art
- [Singer Transformer]
- [PipelineWise Transformations]
- [singer-transform]
- [Meltano Inline Data Mapping]
- [Meltano Inline Stream Maps]
- [AWS DMS source filter rules]
- [AWS DMS table selection and transformation rules]
- ... and many more. Thanks for the inspirations.

## Etymology
With kudos to [Kris Zyp] for conceiving [JSON Pointer].

## More
```{toctree}
:maxdepth: 1

research
backlog
```



[attrs]: https://www.attrs.org/
[AWS DMS source filter rules]: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.Filters.html
[AWS DMS table selection and transformation rules]: https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Tasks.CustomizingTasks.TableMapping.SelectionTransformation.html
[cattrs]: https://catt.rs/
[Kris Zyp]: https://github.com/kriszyp
[JMESPath]: https://jmespath.org/
[JSON Pointer]: https://datatracker.ietf.org/doc/html/rfc6901
[Meltano Inline Data Mapping]: https://docs.meltano.com/guide/mappers/
[Meltano Inline Stream Maps]: https://sdk.meltano.com/en/latest/stream_maps.html
[PipelineWise Transformations]: https://transferwise.github.io/pipelinewise/user_guide/transformations.html
[Python]: https://en.wikipedia.org/wiki/Python_(programming_language)
[Singer Transformer]: https://github.com/singer-io/singer-python/blob/master/singer/transform.py
[singer-transform]: https://github.com/dkarzon/singer-transform
[transon]: https://transon-org.github.io/

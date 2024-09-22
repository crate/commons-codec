# Zyp Example Gallery

This page gives you a hands-on introduction into Zyp Transformations on behalf
of a few example snippets, recipes, and use cases, in order to get you accustomed
to Zyp's capabilities.

If you discover the need for another kind of transformation, or need assistance
crafting transformation rules, please reach out to us on the [issue tracker].

## General Information

- Transformation recipes include a number of transformation rules
- Transformation rules can use different kinds of processors
- Individual rules can be toggled inactive by using the attribute `disabled: true` on them


## Bucket Transformation
A `BucketTransformation` works on **individual data records**, i.e. on a per-record level,
where each record can be a document with child elements, mostly nested.
You can slice into a document by using JSON Pointer, and apply functions from
arbitrary Python modules as transformer functions.

Let's define a basic transformation including three rules.
- Rename the `_id` field to `id`,
- cast its value to `int`, and
- cast the value of the `reading` field to `float`. 

Let's also illustrate that as a basic example of input/output data.
:::::::{grid}
:gutter: 0
:margin: 0
:padding: 0

::::::{grid-item-card} Input Data
A slightly messy collection of records.
```json
[
  {"_id": "123", "reading": "42.42"},
  {"_id": "456", "reading": -84.01}
]
```
::::::
::::::{grid-item-card} Output Data
An edited variant suitable for storing.
```json
[
  {"id": 123, "reading": 42.42},
  {"id": 456, "reading": -84.01}
]
```
::::::
::::::{grid-item-card} Transformation Rules
The Python program can be executed in a Python REPL 1:1.
The YAML file needs to be loaded and applied, but there is
no tutorial yet: Please look at the source and software tests.

:::::{dropdown} Syntax: Python API and YAML format

::::{tab-set-code}
```{code-block} python
# Record-level "Zyp Bucket Transformation" definition.
# Includes column renaming and applying Python converter functions.

from pprint import pprint
from zyp.model.bucket import BucketTransformation, FieldRenamer, ValueConverter

# Consider a slightly messy collection of records.
data_in = [
    {"_id": "123", "name": "device-foo", "reading": "42.42"},
    {"_id": "456", "name": "device-bar", "reading": -84.01},
]

# Define a record-level "bucket transformation".
transformation = BucketTransformation(
    names=FieldRenamer().add(old="_id", new="id"),
    values=ValueConverter()
    .add(pointer="/id", transformer="builtins.int")
    .add(pointer="/reading", transformer="builtins.float"),
)

# Transform data and dump to stdout.
data_out = list(map(transformation.apply, data_in))
pprint(data_out)
```
```{code-block} yaml
# Record-level "Zyp Bucket Transformation" definition.
# Includes column renaming and applying Python converter functions.
---

meta:
  type: zyp-bucket
  version: 1
names:
  rules:
  - new: id
    old: _id
values:
  rules:
  - pointer: /id
    transformer: builtins.int
  - pointer: /reading
    transformer: builtins.float
```
::::
:::::

:::{tip}
Please toggle the "Syntax" dropdown above to inspect usage of the Python API
how to define transformation rules, and the corresponding YAML representation.
:::

::::::
:::::::


## Collection Transformation
A more advanced transformation example for editing a **collection of data records**.

:::::::{grid}
:gutter: 0
:margin: 0
:padding: 0

::::::{grid-item-card} Input Data
Consider a messy collection of input data.
- The actual collection is nested within the top-level `records` element.
- `_id` fields are conveyed in string format.
- `value` fields include both integer and string values.
- `value` fields are fixed-point values, using a scaling factor of `100`.
- The collection includes invalid `null` records.
  Those records usually trip processing when, for example, filtering on object elements.
```json
{
  "message-source": "system-3000",
  "message-type": "eai-warehouse",
  "records": [
    {"_id": "12", "meta": {"name": "foo", "location": "B"}, "data": {"value": "4242"}},
    null,
    {"_id": "34", "meta": {"name": "bar", "location": "BY"}, "data": {"value": -8401}},
    {"_id": "56", "meta": {"name": "baz", "location": "NI"}, "data": {"value": 2323}},
    {"_id": "78", "meta": {"name": "qux", "location": "NRW"}, "data": {"value": -580}},
    null,
    null
  ]
}
```
::::::

::::::{grid-item-card} Output Data
Consider after applying the transformation rules outlined previously, the expected
outcome is a collection of valid records, optionally filtered, and values adjusted
according to relevant type hints and other conversions.
The edited variant is considered suitable for storing into consolidation and
analytics databases.
```json
[
  {"id": 12, "meta": {"name": "foo", "location": "B"}, "data": {"value": 42.42}},
  {"id": 34, "meta": {"name": "bar", "location": "BY"}, "data": {"value": -84.01}}
]
```
::::::

::::::{grid-item-card} Transformation Rules
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

Zyp let's you concisely write those rules down, using the Python language, and will
also let you serialize the transformation description into a text-based format.

The Python program can be executed in a Python REPL 1:1.
The YAML file needs to be loaded and applied, but there is
no tutorial yet: Please look at the source and software tests.

:::::{dropdown} Syntax: Python API and YAML format

::::{tab-set-code}
```{code-block} python
# Collection-level "Zyp Collection Transformation" definition.
# Includes rules for different kinds of transformations and processors.
# Uses all of JMES, jq, and JSON Pointer technologies for demonstration purposes.

from pprint import pprint
from zyp.model.bucket import BucketTransformation, FieldRenamer, ValueConverter
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation

# Consider a slightly messy collection of records.
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

# Define a collection-level "collection transformation", including Bucket- and Moksha-
# transformations
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

# Transform data and dump to stdout.
data_out = transformation.apply(data_in)
pprint(data_out)
```
```{code-block} yaml
# Collection-level "Zyp Collection Transformation" definition.
# Includes rules for different kinds of transformations and processors.
# Uses all of JMES, jq, and JSON Pointer technologies for demonstration purposes.
---

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
::::

In order to serialize the `zyp-collection` transformation description using the Python API,
for example into YAML format, use this code.
```python
print(transformation.to_yaml())
```
:::::

:::{tip}
Please toggle the "Syntax" dropdown above to inspect usage of the Python API
how to define transformation rules, and the corresponding YAML representation.
:::

::::::

:::::::


## Select elements with Moksha/jq
A compact transformation example that uses `jq` to **select elements** of input documents,
sometimes also called "pick fields" or "include columns".

Let's illustrate that as a basic example of input/output data again.
:::::::{grid} 1
:gutter: 0
:margin: 0
:padding: 0

::::::{grid-item-card} Input Data
A collection of records.
```json
[
  {
    "meta": {"id": "Hotzenplotz", "timestamp": 123456789}, 
    "data": {"abc": 123, "def": 456}
  }
]
```
::::::

::::::{grid-item-card} Output Data
An edited variant suitable for storing.
```json
[
  {
    "meta": {"id": "Hotzenplotz"}, 
    "data": {"abc": 123}
  }
]
```
::::::

::::::{grid-item-card} Transformation Rules
The Python program can be executed in a Python REPL 1:1.
The YAML file needs to be loaded and applied, but there is
no tutorial yet: Please look at the source and software tests.

:::::{dropdown} Syntax: Python API and YAML format

::::{tab-set-code}
```{code-block} python
# Collection-level "Zyp Collection Transformation" definition.
# Includes a Moksha/jq transformation rule for including elements.

from pprint import pprint
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation

data_in = [
    {
        "meta": {"id": "Hotzenplotz", "timestamp": 123456789}, 
        "data": {"abc": 123, "def": 456},
    }
]

transformation = CollectionTransformation(
    pre=MokshaTransformation()
    .jq(".[] |= pick(.meta.id, .data.abc)")
)

# Transform data and dump to stdout.
data_out = transformation.apply(data_in)
pprint(data_out)
```
```{code-block} yaml
# Collection-level "Zyp Collection Transformation" definition.
# Includes a Moksha/jq transformation rule for including elements.
---

meta:
  type: zyp-collection
  version: 1
pre:
  rules:
  - expression: .[] |= pick(.meta.id, .data.abc)
    type: jq
```
::::
:::::

:::{tip}
Please toggle the "Syntax" dropdown above to inspect usage of the Python API
how to define transformation rules, and the corresponding YAML representation.
:::
::::::

:::::::


## Drop elements with Moksha/jq
A compact transformation example that uses `jq` to **drop elements** of input documents,
sometimes also called "ignore fields" or "exclude columns".

Let's illustrate that as a basic example of input/output data again.
:::::::{grid} 1
:gutter: 0
:margin: 0
:padding: 0

::::::{grid-item-card} Input Data
A collection of records.
```json
[
  {
    "meta": {"id": "Hotzenplotz", "timestamp": 123456789}, 
    "data": {"abc": 123, "def": 456}
  }
]
```
::::::

::::::{grid-item-card} Output Data
An edited variant suitable for storing.
```json
[
  {
    "meta": {"id": "Hotzenplotz"}, 
    "data": {"abc": 123}
  }
]
```
::::::

::::::{grid-item-card} Transformation Rules
The Python program can be executed in a Python REPL 1:1.
The YAML file needs to be loaded and applied, but there is
no tutorial yet: Please look at the source and software tests.

:::::{dropdown} Syntax: Python API and YAML format

::::{tab-set-code}
```{code-block} python
# Collection-level "Zyp Collection Transformation" definition.
# Includes a Moksha/jq transformation rule for excluding elements.

from pprint import pprint
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation

data_in = [
    {
        "meta": {"id": "Hotzenplotz", "timestamp": 123456789}, 
        "data": {"abc": 123, "def": 456},
    }
]

transformation = CollectionTransformation(
    pre=MokshaTransformation()
    .jq(".[] |= del(.meta.timestamp, .data.def)")
)

# Transform data and dump to stdout.
data_out = transformation.apply(data_in)
pprint(data_out)
```
```{code-block} yaml
# Collection-level "Zyp Collection Transformation" definition.
# Includes a Moksha/jq transformation rule for excluding elements.
---

meta:
  type: zyp-collection
  version: 1
pre:
  rules:
  - expression: .[] |= del(.meta.timestamp, .data.def)
    type: jq
```
::::
:::::

:::{tip}
Please toggle the "Syntax" dropdown above to inspect usage of the Python API
how to define transformation rules, and the corresponding YAML representation.
:::
::::::

:::::::


## Unwrap and flatten with Moksha/jq
A compact transformation example that uses `jq` to:

- Unwrap the actual collection which is nested within the top-level `records` element.
- Flatten the element `nested-list` which contains nested lists.

Let's illustrate that as a basic example of input/output data once again.
:::::::{grid} 1
:gutter: 0
:margin: 0
:padding: 0

::::::{grid-item-card} Input Data
A slightly messy collection of records.
```json
{
  "message-source": "community",
  "message-type": "mixed-pickles",
  "records": [
    {"nested-list": [{"foo": 1}, [{"foo": 2}, {"foo": 3}]]}
  ]
}
```
::::::

::::::{grid-item-card} Output Data
An edited variant suitable for storing.
```json
[
  {"nested-list": [{"foo": 1}, {"foo": 2}, {"foo": 3}]}
]
```
::::::

::::::{grid-item-card} Transformation Rules
The Python program can be executed in a Python REPL 1:1.
The YAML file needs to be loaded and applied, but there is
no tutorial yet: Please look at the source and software tests.

:::::{dropdown} Syntax: Python API and YAML format

::::{tab-set-code}
```{code-block} python
# Collection-level "Zyp Collection Transformation" definition.
# Includes two Moksha/jq transformation rules for unwrapping and flattening.

from pprint import pprint
from zyp.model.collection import CollectionTransformation
from zyp.model.moksha import MokshaTransformation

data_in = {
    "message-source": "community",
    "message-type": "mixed-pickles",
    "records": [
        {"nested-list": [{"foo": 1}, [{"foo": 2}, {"foo": 3}]]},
    ],
}

transformation = CollectionTransformation(
    pre=MokshaTransformation()
    .jq(".records")
    .jq('.[] |= (."nested-list" |= flatten)'),
)

# Transform data and dump to stdout.
data_out = transformation.apply(data_in)
pprint(data_out)
```
```{code-block} yaml
# Collection-level "Zyp Collection Transformation" definition.
# Includes two Moksha/jq transformation rules for unwrapping and flattening.
---

meta:
  type: zyp-collection
  version: 1
pre:
  rules:
  - expression: .records
    type: jq
  - expression: .[] |= (.data."nested-list" |= flatten)
    type: jq
```
::::
:::::

:::{tip}
Please toggle the "Syntax" dropdown above to inspect usage of the Python API
how to define transformation rules, and the corresponding YAML representation.
:::
::::::

:::::::



[issue tracker]: https://github.com/crate/commons-codec/issues

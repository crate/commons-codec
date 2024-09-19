# Zyp Transformations

## About
A data model and implementation for a compact transformation engine 
based on [JSON Pointer] (RFC 6901), [JMESPath], [jq], [transon], and [DWIM].

The reference implementation is written in [Python], using [attrs] and [cattrs].
The design, conventions, and definitions also encourage implementations
in other programming languages.

## Ideas
:Conciseness:
    Define a multistep data refinement process with as little code as possible.
:Flexibility:
    Zyp is a data transformation library that can be used within frameworks and
    ad hoc pipelines equally well. To be invoked, it doesn't need any infrastructure
    services and is pipeline framework agnostic.
:Interoperability:
    Transformation recipe definitions are represented by a concise data model, which
    can be marshalled to/from text-only representations like JSON or YAML, in order to
    a) encourage implementations in other programming languages, and
    b) be transferred, processed and stored by third party systems.
:Performance:
    Depending on how many transformation rules are written in pure Python vs. more
    efficient processors like jqlang or other compiled transformation languages, it
    may be slower or faster. When applicable, hot spots of the library
    may gradually be rewritten in Rust if that topic becomes an issue.
:Immediate:
    Other ETL frameworks and concepts often need to first land your data in the target
    system before applying subsequent transformations. Zyp is working directly within
    the data pipeline, before data is inserted into the target system.

## Design
:Data Model:
    The data model of Zyp is hierarchical: A Zyp project includes definitions for
    multiple Zyp collections, whose includes definitions for possibly multiple sets
    of transformation rules of different kinds, for example multiple items of
    type `BucketTransformation` or `MokshaTransformation`.

:Components and Rules:
    Those transformation components offer different kinds of features, mostly by
    building upon well-known data transformation standards and processors like
    JSON Pointer, `jq`, and friends. The components are configured using rules.

:Phases and Process:
    The transformation process is conducted on behalf of multiple phases that are
    defined by labels like `pre`, `bucket`, `post`, `treatment`, in that order.
    Each phase can include multiple rules of different kinds.


## Synopsis
::::{tab-set}

:::{tab-item} zyp-project
```{code-block} yaml
:caption: A definition for a Zyp project in YAML format.
meta:
  type: zyp-project
  version: 1
collections:
- address:
    container: testdrive-db
    name: foobar-collection
  schema:
    rules:
    - pointer: /some_date
      type: DATETIME
    - pointer: /another_date
      type: DATETIME
  bucket:
    values:
      rules:
      - pointer: /some_date
        transformer: to_unixtime
      - pointer: /another_date
        transformer: to_unixtime
```

:::

:::{tab-item} zyp-collection
```{code-block} yaml
:caption: A definition for a Zyp collection in YAML format.

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
:::

::::


## Example Gallery
In order to learn how to use Zyp, please explore the hands-on example gallery.
```{toctree}
:maxdepth: 2

Examples <examples>
```
You are also welcome to explore and inspect the software test cases to get further
inspirations that might not have been reflected on the documentation yet.
- [tests/zyp]
- [tests/transform/mongodb]
- [tests/transform/test_zyp_generic.py]

## Prior Art
See [research and development notes](project:#zyp-research),
specifically [an introduction and overview about Singer].

## Etymology
With kudos to [Kris Zyp] for conceiving [JSON Pointer] the other day.

```{toctree}
:maxdepth: 1
:hidden:

Research <research>
Backlog <backlog>
```



[An introduction and overview about Singer]: https://github.com/daq-tools/lorrystream/blob/main/doc/singer/intro.md
[attrs]: https://www.attrs.org/
[cattrs]: https://catt.rs/
[DWIM]: https://en.wikipedia.org/wiki/DWIM
[Kris Zyp]: https://github.com/kriszyp
[jq]: https://jqlang.github.io/jq/
[JMESPath]: https://jmespath.org/
[JSON Pointer]: https://datatracker.ietf.org/doc/html/rfc6901
[Python]: https://en.wikipedia.org/wiki/Python_(programming_language)
[tests/zyp]: https://github.com/crate/commons-codec/tree/main/tests/zyp
[tests/transform/mongodb]: https://github.com/crate/commons-codec/tree/main/tests/transform/mongodb
[tests/transform/test_zyp_generic.py]: https://github.com/crate/commons-codec/blob/main/tests/transform/test_zyp_generic.py
[transon]: https://transon-org.github.io/

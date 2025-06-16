# commons-codec

[![Tests](https://github.com/crate/commons-codec/actions/workflows/tests.yml/badge.svg)](https://github.com/crate/commons-codec/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/crate/commons-codec/branch/main/graph/badge.svg)](https://app.codecov.io/gh/crate/commons-codec)
[![Build status (documentation)](https://readthedocs.org/projects/commons-codec/badge/)](https://commons-codec.readthedocs.io/)
[![PyPI Version](https://img.shields.io/pypi/v/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![Python Version](https://img.shields.io/pypi/pyversions/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![PyPI Downloads](https://pepy.tech/badge/commons-codec/month)](https://pepy.tech/project/commons-codec/)
[![Status](https://img.shields.io/pypi/status/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![License](https://img.shields.io/pypi/l/commons-codec.svg)](https://pypi.org/project/commons-codec/)

## About

> A codec is a device or computer program that encodes or decodes a data stream or signal.
> Codec is a portmanteau of coder/decoder.
>
> A coder or encoder encodes a data stream or a signal for transmission or storage,
> [...], and the decoder function reverses the encoding for playback or editing.
>
> -- https://en.wikipedia.org/wiki/Codec

## What's Inside
- [Change Data Capture (CDC)]: **Transformer components** for converging CDC event messages to
  SQL statements.

- A collection of reusable utilities with minimal dependencies for
  **decoding and transcoding** purposes, mostly collected from other projects like
  [Kotori](https://kotori.readthedocs.io/) and [LorryStream](https://lorrystream.readthedocs.io/),
  in order to provide them per standalone package for broader use cases.

- [Tikray], a generic and compact **transformation engine** written in Python, for data
  decoding, encoding, conversion, translation, transformation, and cleansing purposes,
  to be used as a pipeline element for data pre- and/or post-processing.

## Installation
The package is available from [PyPI] at [commons-codec].
To install the most recent version, including support for MongoDB, and Tikray, run:
```shell
pip install --upgrade 'commons-codec[mongodb,tikray]'
```

## Usage
In order to learn how to use the library, please visit the [documentation],
and explore the source code or its [examples].


## Project Information

### Acknowledgements
Kudos to the authors of all the many software components this library is
vendoring and building upon.

### Contributing
The `commons-codec` package is an open source project, and is
[managed on GitHub]. The project is still in its infancy, and
we appreciate contributions of any kind.

### Etymology
The [Apache Commons Codec] library was the inspiration for the name. Otherwise,
both libraries' ingredients don't have anything in common, yet.

### License
The project uses the LGPLv3 license for the whole ensemble. However, individual
portions of the code base are vendored from other Python packages, where
deviating licenses may apply. Please check for detailed license information
within the header sections of relevant files.



[Apache Commons Codec]: https://commons.apache.org/proper/commons-codec/
[Change Data Capture (CDC)]: https://en.wikipedia.org/wiki/Change_data_capture
[commons-codec]: https://pypi.org/project/commons-codec/
[documentation]: https://commons-codec.readthedocs.io/
[examples]: https://github.com/crate/commons-codec/tree/main/examples
[managed on GitHub]: https://github.com/crate/commons-codec
[PyPI]: https://pypi.org/
[Tikray]: https://tikray.readthedocs.io/

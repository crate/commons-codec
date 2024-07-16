# commons-codec

[![Tests](https://github.com/daq-tools/commons-codec/actions/workflows/tests.yml/badge.svg)](https://github.com/daq-tools/commons-codec/actions/workflows/tests.yml)
[![Coverage](https://codecov.io/gh/daq-tools/commons-codec/branch/main/graph/badge.svg)](https://app.codecov.io/gh/daq-tools/commons-codec)
[![Build status (documentation)](https://readthedocs.org/projects/commons-codec/badge/)](https://cratedb.com/docs/commons-codec/)
[![PyPI Version](https://img.shields.io/pypi/v/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![Python Version](https://img.shields.io/pypi/pyversions/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![PyPI Downloads](https://pepy.tech/badge/commons-codec/month)](https://pepy.tech/project/commons-codec/)
[![Status](https://img.shields.io/pypi/status/commons-codec.svg)](https://pypi.org/project/commons-codec/)
[![License](https://img.shields.io/pypi/l/commons-codec.svg)](https://pypi.org/project/commons-codec/)

## About
Data decoding, encoding, conversion, and translation utilities.

> A codec is a device or computer program that encodes or decodes a data stream or signal.
> Codec is a portmanteau of coder/decoder.
>
> A coder or encoder encodes a data stream or a signal for transmission or storage,
> [...], and the decoder function reverses the encoding for playback or editing.
>
> -- https://en.wikipedia.org/wiki/Codec

## Details
A collection of reusable utilities with minimal dependencies for transcoding
purposes, mostly collected from other projects like [Kotori] and [LorryStream],
in order to provide them per standalone package for broader use cases.

## Installation
The package is available from [PyPI] at [commons-codec].
To install the most recent version, run:
```shell
pip install --upgrade commons-codec
```

## License
The project uses the LGPLv3 license for the whole ensemble. However, individual
portions of the code base are vendorized from other Python packages, where
deviating licenses may apply. Please check for detailed license information
within the header sections of relevant files.

## Contributing
The `commons-codec` package is an open source project, and is
[managed on GitHub](https://github.com/daq-tools/commons-codec).
We appreciate contributions of any kind.

## Etymology
The [Apache Commons Codec] library was the inspiration for the name. Otherwise,
both libraries' ingredients don't have anything in common, yet.


[Apache Commons Codec]: https://commons.apache.org/proper/commons-codec/
[commons-codec]: https://pypi.org/project/commons-codec/
[Kotori]: https://github.com/daq-tools/kotori
[LorryStream]: https://github.com/daq-tools/lorrystream/
[PyPI]: https://pypi.org/

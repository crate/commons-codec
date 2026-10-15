# Development Sandbox

Acquire source code, and install development sandbox.
```shell
git clone https://github.com/daq-tools/commons-codec
cd commons-codec
python3 -m venv .venv
source .venv/bin/activate
pip install --editable='.[all,develop,doc,test]'
```

Invoke software tests.
```
poe check
```

Run the tests against a CrateDB version other than `nightly`.
They need CrateDB 6.0.3 or later, or 5.10.13 or later on the 5.10 line.
```shell
CRATEDB_VERSION=6.4.5 poe test
```

Format code.
```shell
poe format
```

Run linter.
```shell
poe lint
```

Build documentation, with live-reloading.
```shell
poe docs-autobuild
```

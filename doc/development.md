# Development Sandbox

Acquire source code, install development sandbox, and invoke software tests.
```shell
git clone https://github.com/daq-tools/commons-codec
cd commons-codec
python3 -m venv .venv
source .venv/bin/activate
pip install --editable='.[all,develop,doc,test]'
poe check
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

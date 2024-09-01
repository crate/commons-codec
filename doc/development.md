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
export TC_KEEPALIVE=true
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

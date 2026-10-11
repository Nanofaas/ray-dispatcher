import importlib
from pathlib import Path


def test_namespace_exports_and_parents():
    package = importlib.import_module("it.unimib.data.nanofaas.ray_dispatcher")
    assert package.Dispatcher
    assert package.Job
    assert package.__file__ is not None
    assert Path(package.__file__).with_name("remote_runner.py").is_file()
    for name in ("it", "it.unimib", "it.unimib.data", "it.unimib.data.nanofaas"):
        assert importlib.import_module(name).__file__ is None

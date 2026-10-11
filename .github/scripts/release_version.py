"""Select releases from the version change across a push to main."""

import os
import re
import subprocess
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib

from packaging.version import Version

from it.unimib.data.nanofaas.ray_dispatcher import __version__


def should_publish(previous: str, current: str, package_version: str) -> bool:
    if current != package_version:
        raise ValueError("pyproject.toml version must match package __version__")
    if Version(current) < Version(previous):
        raise ValueError("Package version must not decrease")
    return Version(current) > Version(previous)


def main() -> None:
    current = tomllib.loads(Path("pyproject.toml").read_text())["project"]["version"]
    previous = current
    if os.environ["GITHUB_EVENT_NAME"] == "push":
        before = os.environ["BEFORE_SHA"]
        if not re.fullmatch(r"[0-9a-f]{40}", before) or before == "0" * 40:
            raise ValueError("Expected the existing main branch's previous commit SHA")
        # The ref is validated; git receives an argument list, not a shell command.
        result = subprocess.run(  # noqa: S603
            ["git", "show", f"{before}:pyproject.toml"],  # noqa: S607
            check=True,
            capture_output=True,
            text=True,
        )
        previous = tomllib.loads(result.stdout)["project"]["version"]
    publish = should_publish(previous, current, __version__)
    with Path(os.environ["GITHUB_OUTPUT"]).open("a") as output:
        output.write(f"publish={str(publish).lower()}\nversion={Version(current)}\n")
    print(f"Version {previous} -> {current}; publish={publish}")


if __name__ == "__main__":
    main()

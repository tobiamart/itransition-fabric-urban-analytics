"""Local secrets (API keys) kept out of Git.

Secrets live only in `.env` in the project root (ignored by Git), one `NAME=value` per line.
Item definitions in `fabric/` refer to them as `{{secret:NAME}}`; fabric_sync.py fills the value
in on deploy and puts the placeholder back on export. Before every commit the pre-commit hook
checks that no secret value is in the staged changes.

    python tools/local_secrets.py install-hook     # once: install the Git pre-commit hook
    python tools/local_secrets.py check            # scan staged changes (what the hook runs)
    python tools/local_secrets.py check FILE ...   # scan files
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = ROOT / ".env"
MIN_LENGTH = 8  # shorter values are too generic to search for

HOOK = """#!/bin/sh
# Installed by tools/local_secrets.py: blocks commits that contain a value from .env
root="$(git rev-parse --show-toplevel)"
py="$root/.venv/Scripts/python.exe"
[ -x "$py" ] || py=python
exec "$py" "$root/tools/local_secrets.py" check
"""


def load() -> dict[str, str]:
    """NAME -> value from .env (empty values are skipped). Values are never printed."""
    values: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8-sig").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, value = line.split("=", 1)
            value = value.strip().strip('"').strip("'")
            if value:
                values[name.strip()] = value
    return values


def get(name: str) -> str:
    value = os.environ.get(name) or load().get(name)
    if not value:
        sys.exit(f"error: secret {name} is not set, add a line {name}=... to .env")
    return value


def searchable(values: dict[str, str] | None = None) -> dict[str, str]:
    return {name: value for name, value in (values if values is not None else load()).items()
            if len(value) >= MIN_LENGTH}


def leaks(text: str, values: dict[str, str] | None = None) -> list[str]:
    """Names of the secrets whose value appears in `text`."""
    return [name for name, value in searchable(values).items() if value in text]


def redact(text: str, values: dict[str, str] | None = None) -> str:
    for name, value in searchable(values).items():
        text = text.replace(value, f"<{name}>")
    return text


def check(files: list[str]) -> int:
    values = searchable()
    if not values:
        return 0
    found = []
    if files:
        for file in files:
            text = Path(file).read_text(encoding="utf-8", errors="ignore")
            found += [(file, name) for name in leaks(text, values)]
    else:
        staged = subprocess.run(
            ["git", "diff", "--cached", "--no-color", "-U0"], cwd=ROOT,
            capture_output=True, text=True, encoding="utf-8", errors="ignore",
        ).stdout
        found += [("the staged changes", name) for name in leaks(staged, values)]
    for where, name in found:
        print(f"blocked: secret {name} found in {where}", file=sys.stderr)
    return 1 if found else 0


def install_hook() -> None:
    hooks = Path(subprocess.run(["git", "rev-parse", "--git-path", "hooks"], cwd=ROOT,
                                capture_output=True, text=True, check=True).stdout.strip())
    hook = (ROOT / hooks if not hooks.is_absolute() else hooks) / "pre-commit"
    hook.write_text(HOOK, encoding="utf-8", newline="\n")
    hook.chmod(0o755)
    print(f"installed {hook}")


if __name__ == "__main__":
    command, *rest = sys.argv[1:] or ["check"]
    if command == "check":
        sys.exit(check(rest))
    if command == "install-hook":
        install_hook()
    else:
        sys.exit(__doc__)

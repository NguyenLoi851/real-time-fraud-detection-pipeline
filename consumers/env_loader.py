from __future__ import annotations

import os
from pathlib import Path


def load_dotenv_if_exists(path: str = "consumers/.env") -> bool:
    env_path = Path(path)
    if not env_path.exists() or not env_path.is_file():
        return False

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key:
            continue

        if len(value) >= 2 and ((value[0] == '"' and value[-1] == '"') or (value[0] == "'" and value[-1] == "'")):
            value = value[1:-1]

        os.environ.setdefault(key, value)

    return True

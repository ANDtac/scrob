"""Latest source/test activity; idle shutdown preserves every source/data volume."""
from pathlib import Path

paths = [path for path in (Path("/devstate/activity"), Path("/devstate/backend.log"),
                          Path("/devstate/frontend.log")) if path.exists()]
for root in ("/work/backend", "/work/frontend/src"):
    paths.extend(path for path in Path(root).rglob("*")
                 if path.is_file() and "__pycache__" not in path.parts)
print(int(max(path.stat().st_mtime for path in paths)))

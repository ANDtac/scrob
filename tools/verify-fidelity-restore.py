"""Restore synthetic development PG data only; compare every table without printing rows."""
import hashlib
import json
import os
import subprocess
from pathlib import Path

if os.environ.get("FIDELITY_TEST_DATABASE") != "synthetic" or os.environ.get("DATABASE_URL") != "postgresql+asyncpg://postgres@127.0.0.1/fidelity_test":
    raise SystemExit("Synthetic-only restore guard rejected environment")

checkpoint = Path("/devstate/checkpoints")
checkpoint.mkdir(exist_ok=True)
dump = checkpoint / "fidelity-test.dump"


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def snapshot(database):
    sql = "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    names = run("psql", "-h", "127.0.0.1", "-U", "postgres", "-d", database, "-Atc", sql).splitlines()
    result = {}
    for name in names:
        if not name.replace("_", "").isalnum():
            raise RuntimeError("Unexpected table identifier")
        output = run("psql", "-h", "127.0.0.1", "-U", "postgres", "-d", database, "-Atc",
                     f'SELECT row_to_json(t)::text FROM public."{name}" t ORDER BY row_to_json(t)::text')
        result[name] = {"rows": len(output.splitlines()), "sha256": hashlib.sha256(output.encode()).hexdigest()}
    return result


before = snapshot("fidelity_test")
run("pg_dump", "-h", "127.0.0.1", "-U", "postgres", "-d", "fidelity_test", "-Fc", "-f", str(dump))
existing = run("psql", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres", "-Atc",
               "SELECT 1 FROM pg_database WHERE datname='fidelity_restore_test'").strip()
if existing:
    raise SystemExit("Retained restore already exists; preserve it and use a deliberate new checkpoint")
run("createdb", "-h", "127.0.0.1", "-U", "postgres", "fidelity_restore_test")
run("pg_restore", "-h", "127.0.0.1", "-U", "postgres", "-d", "fidelity_restore_test", "--exit-on-error", str(dump))
after = snapshot("fidelity_restore_test")
if before != after:
    raise RuntimeError("Restored table hashes differ")
receipt = {"format": 1, "synthetic_only": True, "tables": len(before),
           "all_table_hashes_match": True, "dump_sha256": hashlib.sha256(dump.read_bytes()).hexdigest(),
           "tables_verified": before}
(checkpoint / "restore-receipt.json").write_text(json.dumps(receipt, indent=2))
print(json.dumps({key: value for key, value in receipt.items() if key != "tables_verified"}))

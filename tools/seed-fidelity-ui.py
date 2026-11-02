"""Create an isolated synthetic UI login, storing its password only in dev volume."""
import asyncio
import json
import os
import secrets
from pathlib import Path

from core.security import get_password_hash
from db import AsyncSessionLocal
from models import User


async def main():
    if os.environ.get("DATABASE_URL") != "postgresql+asyncpg://postgres@127.0.0.1/fidelity_test":
        raise RuntimeError("Synthetic UI environment required")
    async with AsyncSessionLocal() as db:
        user = await db.get(User, 1)
        if user.username != "synthetic1" or not user.email.endswith("@example.invalid"):
            raise RuntimeError("Unexpected owner")
        password = secrets.token_urlsafe(32)
        user.password_hash = get_password_hash(password)
        user.is_admin = True
        await db.commit()
        target = Path("/devstate/ui-login.json")
        target.write_text(json.dumps({"username": user.username, "password": password}))
        target.chmod(0o600)
    print("Synthetic login prepared privately; production was not accessed.")


asyncio.run(main())

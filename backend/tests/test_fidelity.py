"""Synthetic PostgreSQL acceptance; no production credentials or external HTTP."""
import asyncio
import os
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from core import fidelity
from fastapi import HTTPException
from fidelity_schemas import CommitRequest, ImportManifest, Occurred
from models import Comment, Media, PlaybackProgress, SourceRecord, SourceRecordVersion, User, WatchEvent
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

pytestmark = pytest.mark.skipif(
    os.environ.get("FIDELITY_TEST_DATABASE") != "synthetic",
    reason="requires explicitly isolated synthetic PostgreSQL database",
)


def manifest(key="one", records=None, source="test", priority="newest_dated"):
    return ImportManifest(source=source, account="synthetic", idempotency_key=key,
        parser_version="test-1", source_sha256="a" * 64, movie_rating_priority=priority,
        records=records or [record()])


def record(identity="watch-1", precision="unknown", value=None, **extra):
    return dict(external_id=identity, kind="watched_event",
        media={"type": "movie", "tmdb_id": 99999001, "title": "Synthetic movie"},
        occurred={"precision": precision, "value": value}, **extra)


async def setup():
    url = os.environ["DATABASE_URL"]
    assert url.endswith("/fidelity_test") and "127.0.0.1" in url
    engine = create_async_engine(url)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as db:
        # This guard is intentionally stricter than accepting an arbitrary env URL.
        await db.execute(text("TRUNCATE users, media, shows RESTART IDENTITY CASCADE"))
        db.add_all([User(id=i, username=f"synthetic{i}", email=f"synthetic{i}@example.invalid",
                        api_key=f"synthetic-api-{i}") for i in (1, 2)])
        await db.commit()
    return engine, sessions


async def import_manifest(db, item, user=1):
    preview = await fidelity.preview(db, user, item)
    request = CommitRequest(**{k: preview[k] for k in ("batch_id", "manifest_sha256", "state_sha256")})
    return await fidelity.commit(db, user, request), request


@pytest.mark.asyncio
async def test_idempotent_dates_rewatches_reviews_no_progress_or_outbound():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            media = Media(id=1, media_type="movie", tmdb_id=99999001, title="Synthetic movie")
            db.add(media)
            await db.flush()
            db.add(PlaybackProgress(user_id=1, media_id=1, progress_seconds=123, progress_percent=12))
            await db.commit()
            rows = [record(), record("watch-2"), record("day", "date", "2022-03-13"),
                    record("local", "local_datetime", "2022-11-06T01:30:00"),
                    record("instant", "instant", "2022-03-13T01:30:00-05:00"),
                    {**record("review"), "kind": "review", "review": "Same review",
                     "linked_event_external_id": "watch-2"}]
            item = manifest(records=rows)
            before = fidelity.row_dict(await db.scalar(select(PlaybackProgress)))
            with patch("httpx.AsyncClient.request", new=AsyncMock(side_effect=AssertionError("no outbound HTTP"))):
                first, request = await import_manifest(db, item)
                repeat = await fidelity.commit(db, 1, request)
            assert repeat == first
            assert first["receipt"]["watches_created"] == 5
            events = (await db.scalars(select(WatchEvent).order_by(WatchEvent.id))).all()
            assert len(events) == 5
            assert events[0].watched_at is None and events[1].watched_at is None
            assert str(events[2].watched_on) == "2022-03-13" and events[2].watched_at is None
            assert events[3].watched_local_at == datetime(2022, 11, 6, 1, 30)
            assert events[4].watched_at == datetime(2022, 3, 13, 6, 30)
            comment = await db.scalar(select(Comment))
            assert comment.watch_event_id == events[1].id
            assert before == fidelity.row_dict(await db.scalar(select(PlaybackProgress)))
            exported = await fidelity.export_owner(db, 1)
            assert len(exported["fidelity_source_versions"]) == 6
            assert exported["comments"][0]["source_occurred"]["precision"] == "unknown"
            assert await db.scalar(select(func.count()).select_from(WatchEvent).where(WatchEvent.user_id == 2)) == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_concurrent_same_batch_single_commit_and_changed_preview_rejected():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            result = await fidelity.preview(db, 1, manifest())
            request = CommitRequest(**{k: result[k] for k in ("batch_id", "manifest_sha256", "state_sha256")})
        async def worker():
            async with sessions() as db:
                return await fidelity.commit(db, 1, request)
        results = await asyncio.gather(worker(), worker())
        assert results[0] == results[1]
        async with sessions() as db:
            assert await db.scalar(select(func.count()).select_from(WatchEvent)) == 1
            result = await fidelity.preview(db, 1, manifest("two", [record("two")]))
            request = CommitRequest(**{k: result[k] for k in ("batch_id", "manifest_sha256", "state_sha256")})
            db.add(WatchEvent(user_id=1, media_id=1, completed=True, watched_at=datetime(2020, 1, 1)))
            await db.commit()
            with pytest.raises(HTTPException, match="history changed"):
                await fidelity.commit(db, 1, request)
            await db.rollback()
            with pytest.raises(HTTPException):
                await fidelity.commit(db, 2, request)
            await db.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_ownership_constraints_and_atomic_rollback():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            await import_manifest(db, manifest())
            event = await db.scalar(select(WatchEvent))
            event_id, media_id = event.id, event.media_id
            forged = manifest("forged", [record("forged", match_event_id=event.id)])
            with pytest.raises(HTTPException):
                await fidelity.preview(db, 2, forged)
            await db.rollback()
            db.add(SourceRecord(user_id=2, source="test", account="other", kind="watched_event",
                                external_id="cross-owner", media_id=media_id, watch_event_id=event_id))
            with pytest.raises(IntegrityError):
                await db.commit()
            await db.rollback()
            result = await fidelity.preview(db, 1, manifest("fault", [record("fault-1"), record("fault-2")]))
            request = CommitRequest(**{k: result[k] for k in ("batch_id", "manifest_sha256", "state_sha256")})
            original = fidelity.find_media
            calls = 0
            async def failing(*args, **kwargs):
                nonlocal calls
                if kwargs.get("create"):
                    calls += 1
                    if calls == 2:
                        raise RuntimeError("injected fault")
                return await original(*args, **kwargs)
            with patch.object(fidelity, "find_media", side_effect=failing):
                with pytest.raises(RuntimeError, match="injected fault"):
                    await fidelity.commit(db, 1, request)
            await db.rollback()
            assert await db.scalar(select(func.count()).select_from(WatchEvent)) == 1
            assert await db.scalar(select(func.count()).select_from(SourceRecordVersion)) == 1
            await fidelity.commit(db, 1, request)
            assert await db.scalar(select(func.count()).select_from(WatchEvent)) == 3
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_assertions_link_without_rewatch_and_rating_revisions_policy():
    from models import Rating, RatingRevision
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            await import_manifest(db, manifest(priority="letterboxd"))
            assertion = {**record("status"), "kind": "watched_assertion"}
            await import_manifest(db, manifest("assert", [assertion], source="tvtime", priority="letterboxd"))
            assert await db.scalar(select(func.count()).select_from(WatchEvent)) == 1
            score = {**record("rating", "date", "2020-01-01"), "kind": "rating", "rating": {"value": "4.5", "scale": "5"}}
            await import_manifest(db, manifest("lb", [score], source="letterboxd", priority="letterboxd"))
            other = {**record("rating", "instant", "2025-01-01T00:00:00Z"), "kind": "rating", "rating": {"value": "2", "scale": "10"}}
            await import_manifest(db, manifest("trakt", [other], source="trakt", priority="letterboxd"))
            assert (await db.scalar(select(Rating))).rating == 9
            assert await db.scalar(select(func.count()).select_from(RatingRevision)) == 2
            changed = {**score, "rating": {"value": "5", "scale": "5"}, "occurred": {"precision": "date", "value": "2021-01-01"}}
            await import_manifest(db, manifest("lb2", [changed], source="letterboxd", priority="letterboxd"))
            assert (await db.scalar(select(Rating))).rating == 10
            assert await db.scalar(select(func.count()).select_from(RatingRevision)) == 3
    finally:
        await engine.dispose()


def test_temporal_validation():
    with pytest.raises(ValueError):
        Occurred(precision="instant", value="2020-01-01T12:00:00")
    with pytest.raises(ValueError):
        Occurred(precision="date", value="2020-01-01T00:00:00")
    assert Occurred(precision="local_datetime", value="2020-11-01T01:30:00").timezone is None


@pytest.mark.asyncio
async def test_native_rating_writer_preserves_revision_and_priority():
    from core.fidelity_ratings import install
    from models import Rating, RatingRevision
    install()
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            score = {**record("rating", "date", "2020-01-01"), "kind": "rating", "rating": {"value": "4", "scale": "5"}}
            await import_manifest(db, manifest("lb", [score], source="letterboxd", priority="letterboxd"))
            current = await db.scalar(select(Rating))
            chosen = current.selected_revision_id
            current.rating = 2
            current.rated_at = datetime(2025, 1, 1)
            current._fidelity_source = "trakt"
            await db.commit()
            assert current.rating == 8 and current.selected_revision_id == chosen
            assert await db.scalar(select(func.count()).select_from(RatingRevision)) == 2
            latest = await db.scalar(select(RatingRevision).order_by(RatingRevision.id.desc()))
            assert latest.source == "trakt" and latest.normalized_value == 2
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_projection_delete_preserves_evidence_and_source_artifact_roundtrip():
    import hashlib

    from fidelity_schemas import SourceArtifactUpload
    from routers.fidelity import artifact, upload_artifact
    from sqlalchemy import delete
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            await import_manifest(db, manifest(records=[record(),
                {**record("review"), "kind": "review", "review": "Synthetic text", "linked_event_external_id": "watch-1"}]))
            await db.execute(delete(WatchEvent).where(WatchEvent.user_id == 1))
            await db.commit()
            db.expire_all()
            assert await db.scalar(select(func.count()).select_from(SourceRecordVersion)) == 2
            assert (await db.scalar(select(Comment))).watch_event_id is None
            content = 'source,row\r\n"unmapped","<script>never executes</script>"\r\n'
            upload = SourceArtifactUpload(source="tvtime", account="synthetic", filename="source.csv",
                sha256=hashlib.sha256(content.encode()).hexdigest(), media_type="text/csv", content=content)
            user = await db.get(User, 1)
            result = await upload_artifact(upload, db, user)
            assert (await upload_artifact(upload, db, user))["id"] == result["id"]
            response = await artifact(result["id"], db, user)
            assert response.body == content.encode()
            assert response.headers["x-content-type-options"] == "nosniff"
            other = await db.get(User, 2)
            with pytest.raises(HTTPException):
                await artifact(result["id"], db, other)
            exported = await fidelity.export_owner(db, 1)
            assert exported["fidelity_source_artifacts"][0]["content"] == content
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_episode_parent_identity_cannot_be_spoofed():
    from fidelity_schemas import MediaIdentity
    from models import Show
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            show = Show(id=1, tmdb_id=88888001, tvdb_id=77777001, title="Synthetic series")
            db.add(show)
            await db.flush()
            db.add(Media(media_type="episode", tmdb_id=99999002, title="Synthetic episode",
                         show_id=1, season_number=1, episode_number=1))
            await db.commit()
            base = dict(type="episode", title="Synthetic episode", tmdb_id=99999002,
                        show_tmdb_id=88888001, season_number=1, episode_number=1)
            assert (await fidelity.find_media(db, MediaIdentity(**base))).show_id == 1
            for changes in ({"show_tmdb_id": 123456}, {"show_tvdb_id": 123456}):
                with pytest.raises(HTTPException):
                    await fidelity.find_media(db, MediaIdentity(**{**base, **changes}))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_authenticated_api_rejects_anonymous_and_other_owner():
    import httpx
    from core.security import create_access_token
    from db import get_db
    from fastapi import FastAPI
    from routers.fidelity import router
    engine, sessions = await setup()
    async def dependency():
        async with sessions() as db:
            yield db
    app = FastAPI()
    app.include_router(router, prefix="/imports/v1")
    app.dependency_overrides[get_db] = dependency
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://synthetic") as client:
            assert (await client.get("/imports/v1/evidence")).status_code == 401
            owner = {"Authorization": "Bearer " + create_access_token("1")}
            other = {"Authorization": "Bearer " + create_access_token("2")}
            response = await client.post("/imports/v1/preview", json=manifest().model_dump(mode="json"), headers=owner)
            assert response.status_code == 200
            receipt = response.json()
            body = {k: receipt[k] for k in ("batch_id", "manifest_sha256", "state_sha256")}
            assert (await client.post("/imports/v1/commit", json=body, headers=other)).status_code == 404
            response = await client.post("/imports/v1/commit", json=body, headers=owner)
            assert response.status_code == 200
            assert response.headers["cache-control"] == "no-store"
            assert (await client.get("/imports/v1/evidence", headers=other)).json() == []
            assert (await client.get(f"/imports/v1/batches/{receipt['batch_id']}", headers=other)).status_code == 404
            exported = (await client.get("/imports/v1/export", headers=owner)).json()
            assert len(exported["watch_events"]) == 1
            assert not any("token" in key for key in exported)
    finally:
        await engine.dispose()

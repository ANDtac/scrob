"""Status reconciliation on synthetic PostgreSQL: no household source data."""
import asyncio
import hashlib
import json
from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest
from core import fidelity, fidelity_reconcile
from fastapi import HTTPException
from fidelity_schemas import CommitRequest, StatusReconciliation
from models import Comment, Media, PlaybackProgress, Show, SourceArtifact, SourceRecord, SourceRecordVersion, WatchEvent
from models.rewatch import RewatchProgress, ShowRewatch
from sqlalchemy import select
from tests.test_fidelity import import_manifest, manifest, record, setup
from tests.test_fidelity import pytestmark as fidelity_pytestmark

pytestmark = fidelity_pytestmark


def test_cloud_transport_milliseconds_are_exact_without_float_rounding():
    assert fidelity_reconcile.cloud_milliseconds("1970-01-01T00:00:00.000999Z") == 0
    assert fidelity_reconcile.cloud_milliseconds("1970-01-01T00:00:00.001000Z") == 1
    assert fidelity_reconcile.cloud_milliseconds("1969-12-31T23:59:59.999999Z") == -1
    assert fidelity_reconcile.cloud_milliseconds("2026-01-01T01:00:00.999999+01:00") == 1767225600999
    assert fidelity_reconcile.cloud_milliseconds(None) is None


async def fixture(db):
    db.add_all([Media(id=1, media_type="movie", tmdb_id=99999001, title="Synthetic movie"),
                Media(id=2, media_type="movie", tmdb_id=99999002, title="Unmatched synthetic")])
    await db.flush()
    events = [WatchEvent(id=1, user_id=1, media_id=1, source="stremio", completed=True,
                         watched_precision="legacy", watched_at=datetime(2025, 1, 1)),
              WatchEvent(id=2, user_id=1, media_id=2, source="stremio", completed=True,
                         watched_precision="legacy", watched_at=datetime(2025, 1, 2)),
              WatchEvent(id=3, user_id=2, media_id=1, source="stremio", completed=True,
                         watched_precision="legacy", watched_at=datetime(2025, 1, 3)),
              WatchEvent(id=4, user_id=1, media_id=1, source="manual", completed=True,
                         watched_precision="legacy", watched_at=datetime(2025, 1, 4))]
    db.add_all(events)
    db.add(PlaybackProgress(user_id=1, media_id=1, progress_seconds=123, progress_percent=12))
    await db.commit()
    # Explicit fixture IDs above require matching PostgreSQL identity sequence.
    from sqlalchemy import text
    await db.execute(text("SELECT setval(pg_get_serial_sequence('watch_events','id'), 4)"))
    await db.commit()
    await import_manifest(db, manifest("trakt-events", [
        record("history:11", "instant", "2020-01-01T00:00:00Z"),
        record("history:12", "instant", "2021-01-01T00:00:00Z")], source="trakt"))
    rows = [fidelity.row_dict(event) for event in events if event.user_id == 1]
    content = json.dumps({"format": "scrob-status-projections", "user_id": 1, "watch_events": rows})
    artifact = SourceArtifact(user_id=1, source="stremio", account="synthetic", filename="original.json",
        sha256=hashlib.sha256(content.encode()).hexdigest(), media_type="application/json", content=content)
    db.add(artifact)
    await db.commit()
    return StatusReconciliation(idempotency_key="reconcile-one", account="synthetic", trakt_account="synthetic",
                                artifact_id=artifact.id, artifact_sha256=artifact.sha256, event_ids=[1]), rows[0]


@pytest.mark.asyncio
async def test_reconcile_idempotence_ownership_progress_and_original_evidence():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            request, original = await fixture(db)
            progress = fidelity.row_dict(await db.scalar(select(PlaybackProgress)))
            before = {row.id: fidelity.row_dict(row) for row in (await db.scalars(select(WatchEvent))).all()}
            preview = await fidelity_reconcile.preview(db, 1, request)
            commit = CommitRequest(**{key: preview[key] for key in ("batch_id", "manifest_sha256", "state_sha256")})
        async def worker():
            async with sessions() as db:
                return await fidelity_reconcile.commit(db, 1, commit)
        with patch("httpx.AsyncClient.request", new=AsyncMock(side_effect=AssertionError("no outbound"))):
            results = await asyncio.gather(worker(), worker())
        assert results[0] == results[1] and results[0]["receipt"]["status_projections_removed"] == 1
        async with sessions() as db:
            after = {row.id: fidelity.row_dict(row) for row in (await db.scalars(select(WatchEvent))).all()}
            assert after == {key: value for key, value in before.items() if key != 1}
            assert progress == fidelity.row_dict(await db.scalar(select(PlaybackProgress)))
            source = await db.scalar(select(SourceRecord).where(SourceRecord.kind == "watched_assertion"))
            version = await db.scalar(select(SourceRecordVersion).where(SourceRecordVersion.record_id == source.id))
            assert source.watch_event_id is None and source.media_id == 1
            assert version.evidence["payload"]["original_watch_event"] == original
            assert version.evidence["occurred"] == {"precision": "unknown", "value": None}
            assert not version.evidence["payload"]["same_viewing_equivalence_claimed"]
            assert len(version.evidence["payload"]["supporting_trakt_source_record_ids"]) == 2
            assert await fidelity_reconcile.preview(db, 1, request) == results[0]
            from routers.sync import _apply_nuvio_watch_history
            old_status = {"content_id": "tt99999001", "content_type": "movie", "watched_at": 1735689600000}
            newer = {**old_status, "watched_at": 1767225600000}
            # The migrated status must not return at the next inbound pull;
            # a genuinely different later timestamp remains a normal viewing.
            with patch("httpx.AsyncClient.request", new=AsyncMock(side_effect=AssertionError("no outbound"))):
                assert await _apply_nuvio_watch_history(db, 1, [old_status], {}, {"tt99999001": 99999001}) == set()
                assert await _apply_nuvio_watch_history(db, 1, [newer], {}, {"tt99999001": 99999001}) == {1}
            assert progress == fidelity.row_dict(await db.scalar(select(PlaybackProgress)))
            with pytest.raises(HTTPException):
                await fidelity_reconcile.commit(db, 2, commit)
            await db.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_rejects_changed_unmatched_manual_cross_owner_and_references():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            request, _ = await fixture(db)
            for ids in ([2], [3], [4]):
                with pytest.raises(HTTPException):
                    await fidelity_reconcile.preview(db, 1, request.model_copy(update={"event_ids": ids}))
                await db.rollback()
            with pytest.raises(HTTPException):
                await fidelity_reconcile.preview(db, 2, request)
            await db.rollback()
            preview = await fidelity_reconcile.preview(db, 1, request)
            commit = CommitRequest(**{key: preview[key] for key in ("batch_id", "manifest_sha256", "state_sha256")})
            event = await db.get(WatchEvent, 1)
            event.watched_at = datetime(2026, 1, 1)
            await db.commit()
            with pytest.raises(HTTPException, match="changed"):
                await fidelity_reconcile.commit(db, 1, commit)
            await db.rollback()
            event = await db.get(WatchEvent, 1)
            event.watched_at = datetime(2025, 1, 1)
            db.add(Comment(user_id=1, media_type="movie", tmdb_id=99999001, content="Linked synthetic review", watch_event_id=1))
            await db.commit()
            with pytest.raises(HTTPException, match="references"):
                await fidelity_reconcile.preview(db, 1, request)
            await db.rollback()
            linked = await db.scalar(select(Comment))
            await db.delete(linked)
            db.add(Show(id=1, title="Synthetic show", tmdb_id=99999003))
            await db.flush()
            db.add(ShowRewatch(id=1, user_id=1, show_id=1))
            await db.flush()
            db.add(RewatchProgress(rewatch_id=1, media_id=1, watch_event_id=1))
            await db.commit()
            with pytest.raises(HTTPException, match="references"):
                await fidelity_reconcile.preview(db, 1, request)
            await db.rollback()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_reconcile_failure_rolls_back_deletion_and_evidence():
    engine, sessions = await setup()
    try:
        async with sessions() as db:
            request, _ = await fixture(db)
            preview = await fidelity_reconcile.preview(db, 1, request)
            commit = CommitRequest(**{key: preview[key] for key in ("batch_id", "manifest_sha256", "state_sha256")})
            original_delete = db.delete
            async def failed_delete(event):
                await original_delete(event)
                raise RuntimeError("injected failure")
            with patch.object(db, "delete", side_effect=failed_delete):
                with pytest.raises(RuntimeError, match="injected failure"):
                    await fidelity_reconcile.commit(db, 1, commit)
            await db.rollback()
            assert await db.get(WatchEvent, 1) is not None
            assert await db.scalar(select(SourceRecord).where(SourceRecord.kind == "watched_assertion")) is None
            result = await fidelity_reconcile.commit(db, 1, commit)
            assert result["receipt"]["original_rows_preserved"] == 1
    finally:
        await engine.dispose()

"""Preserve imported status assertions without counting them as extra viewings.

This deliberately does not equate any legacy timestamp with a Trakt viewing.
Original rows remain immutable evidence attached to media, never to an arbitrary
viewing. Only explicitly selected, unreferenced legacy Stremio projections can
be removed, after verified Trakt viewing evidence exists for that exact media.
"""
import hashlib
import json
from datetime import datetime, timezone

from core import fidelity
from fastapi import HTTPException
from fidelity_schemas import CommitRequest, Occurred, StatusReconciliation
from models import Comment, ImportBatch, Media, Show, SourceArtifact, SourceRecord, SourceRecordVersion, WatchEvent
from models.rewatch import RewatchProgress
from sqlalchemy import select, text

OPERATION = "reconcile_stremio_status_v1"


def cloud_milliseconds(value):
    """Transport deduplication only; does not assert a historical viewing date."""
    if value is None:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    delta = value.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (delta.days * 86400 + delta.seconds) * 1000 + delta.microseconds // 1000


async def preserved_status_times(db, user_id, media_ids):
    rows = (await db.execute(select(SourceRecord.media_id, SourceRecordVersion.evidence)
        .join(SourceRecordVersion, SourceRecordVersion.record_id == SourceRecord.id)
        .join(ImportBatch, ImportBatch.id == SourceRecordVersion.batch_id)
        .where(SourceRecord.user_id == user_id, SourceRecordVersion.user_id == user_id,
               ImportBatch.user_id == user_id, SourceRecord.source == "stremio",
               SourceRecord.kind == "watched_assertion", SourceRecord.media_id.in_(media_ids),
               ImportBatch.status == "committed", ImportBatch.manifest["operation"].astext == OPERATION))).all()
    result = {}
    for media_id, evidence in rows:
        original = evidence.get("payload", {}).get("original_watch_event", {})
        if (original.get("user_id") == user_id and original.get("media_id") == media_id
                and original.get("watched_at") is not None):
            result.setdefault(media_id, set()).add(cloud_milliseconds(original.get("watched_at")))
    return result


async def candidates(db, user_id, request):
    artifact = await db.scalar(select(SourceArtifact).where(SourceArtifact.id == request.artifact_id,
        SourceArtifact.user_id == user_id, SourceArtifact.source == "stremio",
        SourceArtifact.account == request.account, SourceArtifact.sha256 == request.artifact_sha256))
    if artifact is None or hashlib.sha256(artifact.content.encode()).hexdigest() != request.artifact_sha256:
        raise HTTPException(409, "Original status artifact missing or changed")
    try:
        preserved = json.loads(artifact.content)
        originals = {row["id"]: row for row in preserved["watch_events"]}
        if (preserved["format"] != "scrob-status-projections" or preserved["user_id"] != user_id
                or len(originals) != len(preserved["watch_events"])):
            raise ValueError
    except (KeyError, TypeError, ValueError):
        raise HTTPException(409, "Original status artifact format invalid") from None
    events = (await db.scalars(select(WatchEvent).where(WatchEvent.user_id == user_id,
        WatchEvent.id.in_(request.event_ids)).order_by(WatchEvent.id))).all()
    if len(events) != len(request.event_ids):
        raise HTTPException(409, "Selected status projection missing or not owned")
    for event in events:
        if (event.source != "stremio" or event.watched_precision != "legacy" or not event.completed
                or fidelity.row_dict(event) != originals.get(event.id)):
            raise HTTPException(409, "Selected projection is not the preserved legacy Stremio status")
    for model in (Comment, SourceRecord, RewatchProgress):
        if await db.scalar(select(model.id).where(model.watch_event_id.in_(request.event_ids)).limit(1)):
            raise HTTPException(409, "Selected projection has references requiring separate review")
    # Inspect the latest immutable source version, not a stale revised assertion.
    rows = (await db.execute(select(SourceRecord, SourceRecordVersion, WatchEvent)
        .join(SourceRecordVersion, SourceRecordVersion.record_id == SourceRecord.id)
        .join(WatchEvent, WatchEvent.id == SourceRecord.watch_event_id)
        .where(SourceRecord.user_id == user_id, SourceRecordVersion.user_id == user_id,
               WatchEvent.user_id == user_id, SourceRecord.source == "trakt",
               SourceRecord.account == request.trakt_account, SourceRecord.kind == "watched_event",
               SourceRecord.media_id.in_({event.media_id for event in events}))
        .order_by(SourceRecordVersion.id))).all()
    latest = {record.id: (record, version, event) for record, version, event in rows}
    verified = {}
    for record, version, event in latest.values():
        occurrence = version.evidence.get("occurred", {})
        if (event.completed and event.watched_precision == "instant" and event.media_id == record.media_id
                and record.external_id.startswith("history:") and occurrence.get("precision") == "instant"
                and fidelity.temporal_columns(Occurred.model_validate(occurrence))["watched_at"] == event.watched_at):
            verified.setdefault(event.media_id, []).append(record.id)
    if any(event.media_id not in verified for event in events):
        raise HTTPException(409, "Verified Trakt viewing evidence missing for selected media")
    return events, verified


async def preview(db, user_id, request: StatusReconciliation):
    await fidelity.owner_lock(db, user_id)
    encoded = {**request.model_dump(mode="json"), "operation": OPERATION, "source": "stremio"}
    checksum = fidelity.digest(encoded)
    existing = await db.scalar(select(ImportBatch).where(ImportBatch.user_id == user_id,
        ImportBatch.idempotency_key == request.idempotency_key))
    if existing and existing.manifest_sha256 != checksum:
        raise HTTPException(409, "Reconciliation key belongs to a different request")
    if existing and existing.status == "committed":
        return fidelity.batch_summary(existing)
    events, _ = await candidates(db, user_id, request)
    state = await fidelity.state_digest(db, user_id)
    receipt = {"status_projections_selected": len(events), "original_rows_preserved": len(events),
               "same_viewing_links": 0, "new_viewings": 0}
    if existing:
        batch = existing
        batch.state_sha256, batch.receipt = state, receipt
    else:
        batch = ImportBatch(user_id=user_id, idempotency_key=request.idempotency_key, manifest=encoded,
                            manifest_sha256=checksum, state_sha256=state, status="preview", receipt=receipt)
        db.add(batch)
    await db.commit()
    return fidelity.batch_summary(batch)


async def commit(db, user_id, request: CommitRequest):
    await fidelity.owner_lock(db, user_id)
    batch = await db.scalar(select(ImportBatch).where(ImportBatch.id == request.batch_id,
        ImportBatch.user_id == user_id).with_for_update())
    if batch is None:
        raise HTTPException(404, "Reconciliation batch not found")
    if batch.manifest.get("operation") != OPERATION:
        raise HTTPException(409, "Batch is not a status reconciliation")
    if request.manifest_sha256 != batch.manifest_sha256 or request.state_sha256 != batch.state_sha256:
        raise HTTPException(409, "Reconciliation preview receipt mismatch")
    if batch.status == "committed":
        return fidelity.batch_summary(batch)
    await fidelity.import_write_lock(db)
    if db.bind.dialect.name == "postgresql":
        # This child FK cascades on deletion; a concurrent native rewatch writer
        # must not add a reference after the explicit refusal check below.
        await db.execute(text("LOCK TABLE rewatch_progress IN SHARE ROW EXCLUSIVE MODE"))
    if await fidelity.state_digest(db, user_id) != batch.state_sha256:
        raise HTTPException(409, "History changed since reconciliation preview")
    body = StatusReconciliation.model_validate({key: value for key, value in batch.manifest.items()
                                               if key not in ("operation", "source")})
    events, verified = await candidates(db, user_id, body)
    for event in events:
        original = fidelity.row_dict(event)
        media = await db.get(Media, event.media_id)
        parent = await db.get(Show, media.show_id) if media.show_id else None
        identity = {"type": media.media_type.value, "title": media.title,
                    **{key: getattr(media, key) for key in ("tmdb_id", "tvdb_id", "imdb_id", "season_number", "episode_number")}}
        if parent:
            identity.update(show_tmdb_id=parent.tmdb_id, show_tvdb_id=parent.tvdb_id, show_title=parent.title)
        evidence = {"external_id": f"legacy-status:{event.id}", "kind": "watched_assertion", "media": identity,
                    "occurred": {"precision": "unknown", "value": None},
                    "source_sha256": body.artifact_sha256,
                    "locator": f"watch_events[id={event.id}]",
                    "payload": {"original_watch_event": original, "artifact_id": body.artifact_id,
                                "reason": "Imported Stremio status is not an additional distinct viewing.",
                                "supporting_trakt_source_record_ids": sorted(verified[event.media_id]),
                                "same_viewing_equivalence_claimed": False}}
        source = SourceRecord(user_id=user_id, source="stremio", account=body.account,
                              kind="watched_assertion", external_id=evidence["external_id"],
                              media_id=media.id, watch_event_id=None)
        db.add(source)
        await db.flush()
        db.add(SourceRecordVersion(user_id=user_id, record_id=source.id, batch_id=batch.id,
                                    payload_sha256=fidelity.digest(evidence), evidence=evidence))
        await db.flush()
        await db.delete(event)
    await db.flush()
    batch.status = "committed"
    batch.committed_at = datetime.now(timezone.utc)
    batch.receipt = {"status_projections_removed": len(events), "source_assertions_created": len(events),
                     "original_rows_preserved": len(events), "same_viewing_links": 0,
                     "new_viewings": 0, "progress_writes": 0, "outbound_requests": 0}
    await db.commit()
    return fidelity.batch_summary(batch)

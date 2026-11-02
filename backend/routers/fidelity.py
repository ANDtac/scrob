"""Private, authenticated import/evidence endpoints. No user-id override."""
import hashlib

from core import fidelity, fidelity_reconcile
from db import get_db
from dependencies import get_current_user
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from fastapi.responses import FileResponse
from fidelity_schemas import CommitRequest, ImportManifest, SourceArtifactUpload, StatusReconciliation
from models import (
    ImportBatch,
    Rating,
    RatingRevision,
    SourceArtifact,
    SourceRecord,
    SourceRecordVersion,
    User,
    WatchEvent,
)
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


def private_response(response: Response):
    response.headers["Cache-Control"] = "no-store"


router = APIRouter(dependencies=[Depends(private_response)])


class PrivateExportResponse(FileResponse):
    """Remove the private snapshot even when delivery is cancelled."""

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            from pathlib import Path
            Path(self.path).unlink(missing_ok=True)


@router.get("/summary")
async def summary(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    rows = (await db.execute(select(WatchEvent.watched_precision, func.count()).where(
        WatchEvent.user_id == user.id).group_by(WatchEvent.watched_precision))).all()
    return {"watch_precision_counts": dict(rows), "source_records": await db.scalar(select(func.count()).select_from(SourceRecord).where(SourceRecord.user_id == user.id)),
            "rating_revisions": await db.scalar(select(func.count()).select_from(RatingRevision).where(RatingRevision.user_id == user.id))}


@router.get("/ratings")
async def ratings(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user),
                  offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    rows = (await db.scalars(select(RatingRevision).where(RatingRevision.user_id == user.id)
        .order_by(RatingRevision.id.desc()).offset(offset).limit(limit))).all()
    selected = set((await db.scalars(select(Rating.selected_revision_id).where(Rating.user_id == user.id))).all())
    return [{**fidelity.row_dict(row), "selected": row.id in selected} for row in rows]


@router.post("/artifacts")
async def upload_artifact(body: SourceArtifactUpload, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    if hashlib.sha256(body.content.encode()).hexdigest() != body.sha256:
        raise HTTPException(422, "Source artifact checksum mismatch")
    await fidelity.owner_lock(db, user.id)
    artifact = await db.scalar(select(SourceArtifact).where(SourceArtifact.user_id == user.id,
        SourceArtifact.source == body.source, SourceArtifact.account == body.account, SourceArtifact.sha256 == body.sha256))
    if artifact is None:
        artifact = SourceArtifact(user_id=user.id, **body.model_dump())
        db.add(artifact)
        await db.commit()
    return {key: value for key, value in fidelity.row_dict(artifact).items() if key != "content"}


@router.get("/artifacts")
async def artifacts(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    rows = (await db.scalars(select(SourceArtifact).where(SourceArtifact.user_id == user.id).order_by(SourceArtifact.id))).all()
    return [{key: value for key, value in fidelity.row_dict(row).items() if key != "content"} for row in rows]


@router.get("/artifacts/{artifact_id}")
async def artifact(artifact_id: int, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    row = await db.scalar(select(SourceArtifact).where(SourceArtifact.user_id == user.id, SourceArtifact.id == artifact_id))
    if row is None:
        raise HTTPException(404, "Source artifact not found")
    # Serve plain text with nosniff: imported source must never execute as HTML.
    return Response(row.content, media_type="text/plain", headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})


@router.post("/preview")
async def preview(body: ImportManifest, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    try:
        return await fidelity.preview(db, user.id, body)
    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        raise HTTPException(409, "Preview failed; no changes committed") from None


@router.post("/commit")
async def commit(body: CommitRequest, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    try:
        return await fidelity.commit(db, user.id, body)
    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        # SQL/driver exceptions can contain raw bound source rows. Do not send
        # or log those payloads as an unhandled traceback.
        raise HTTPException(409, "Import transaction failed; no changes committed") from None


@router.post("/status-reconciliation/preview")
async def reconcile_preview(body: StatusReconciliation, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    try:
        return await fidelity_reconcile.preview(db, user.id, body)
    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        raise HTTPException(409, "Status reconciliation preview failed; no changes committed") from None


@router.post("/status-reconciliation/commit")
async def reconcile_commit(body: CommitRequest, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    try:
        return await fidelity_reconcile.commit(db, user.id, body)
    except HTTPException:
        await db.rollback()
        raise
    except Exception:
        await db.rollback()
        raise HTTPException(409, "Status reconciliation failed; no changes committed") from None


@router.get("/batches")
async def batches(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user),
                  offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    rows = (await db.scalars(select(ImportBatch).where(ImportBatch.user_id == user.id)
        .order_by(ImportBatch.id.desc()).offset(offset).limit(limit))).all()
    return [fidelity.batch_summary(row) for row in rows]


@router.get("/batches/{batch_id}")
async def batch(batch_id: int, db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    row = await db.scalar(select(ImportBatch).where(ImportBatch.id == batch_id, ImportBatch.user_id == user.id))
    if row is None:
        raise HTTPException(404, "Import batch not found")
    return {**fidelity.batch_summary(row), "manifest": row.manifest}


@router.get("/evidence")
async def evidence(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user),
                   watch_event_id: int | None = Query(None, gt=0),
                   offset: int = Query(0, ge=0), limit: int = Query(50, ge=1, le=200)):
    query = select(SourceRecord).where(SourceRecord.user_id == user.id)
    if watch_event_id:
        query = query.where(SourceRecord.watch_event_id == watch_event_id)
    rows = (await db.scalars(query.order_by(SourceRecord.id).offset(offset).limit(limit))).all()
    versions = (await db.scalars(select(SourceRecordVersion).where(SourceRecordVersion.user_id == user.id,
        SourceRecordVersion.record_id.in_([row.id for row in rows])).order_by(SourceRecordVersion.id))).all()
    return [{**fidelity.row_dict(row), "versions": [fidelity.row_dict(v) for v in versions if v.record_id == row.id]} for row in rows]


@router.get("/export")
async def export(db: AsyncSession = Depends(get_db), user: User = Depends(get_current_user)):
    filename = await fidelity.export_owner_file(db, user.id)
    return PrivateExportResponse(filename, media_type="application/json", filename="scrob-fidelity.json",
                                 headers={"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff"})

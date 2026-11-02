"""Transactional history import. No playback, scheduler or outbound-sync dependencies.

Every source row is preserved; canonical projections are deliberately conservative.
Metadata identity resolution is local and explicit: this module does no HTTP I/O.
"""
import hashlib
import json
import os
import tempfile
from collections import Counter
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

from fastapi import HTTPException
from fidelity_schemas import CommitRequest, ImportManifest, Occurred
from models import (
    Collection,
    Comment,
    ImportBatch,
    Media,
    MediaType,
    Rating,
    RatingPolicy,
    RatingRevision,
    Show,
    SourceArtifact,
    SourceRecord,
    SourceRecordVersion,
    User,
    WatchEvent,
)
from sqlalchemy import func, or_, select, text, union
from sqlalchemy.ext.asyncio import AsyncSession


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False, default=str).encode()).hexdigest()


def row_dict(row) -> dict:
    def encode(value):
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, Decimal):
            return str(value)
        return value.value if hasattr(value, "value") else value
    return {c.name: encode(getattr(row, c.name)) for c in row.__table__.columns}


def record_evidence(record) -> dict:
    encoded = record.model_dump(mode="json")
    # Preserve hashes of schema1 manifests created before this optional field.
    if encoded.get("minimum_known_viewings") is None:
        encoded.pop("minimum_known_viewings", None)
    return encoded


def temporal_columns(occurred: Occurred) -> dict:
    result = dict(watched_at=None, watched_precision=occurred.precision,
                  watched_on=None, watched_local_at=None, watched_timezone=occurred.timezone)
    if occurred.precision == "instant":
        result["watched_at"] = datetime.fromisoformat(occurred.value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
    elif occurred.precision == "date":
        result["watched_on"] = date.fromisoformat(occurred.value)
    elif occurred.precision == "local_datetime":
        result["watched_local_at"] = datetime.fromisoformat(occurred.value)
    return result


def temporal_bounds(occurred: dict):
    """Comparable date intervals. Unknown zones/precision never invent ordering."""
    precision, value = occurred.get("precision"), occurred.get("value")
    if precision == "instant" and value:
        stamp = datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        return stamp, stamp
    if precision == "date" and value:
        # A date without a timezone spans all legal timezone offsets.
        from datetime import timedelta
        stamp = datetime.combine(date.fromisoformat(value), datetime.min.time(), timezone.utc)
        return stamp - timedelta(hours=14), stamp + timedelta(days=1, hours=12)
    return None


async def owner_lock(db: AsyncSession, user_id: int):
    # Row locking is portable and serializes previews/commits for this owner only.
    user = await db.scalar(select(User).where(User.id == user_id).with_for_update())
    if user is None:
        raise HTTPException(404, "User not found")


async def import_write_lock(db: AsyncSession):
    # Controlled, bounded import only. These locks also exclude legacy writers
    # which do not yet participate in owner-level locking. The UI/app remains
    # readable; writers wait until this short transaction commits or rolls back.
    # Fixed acquisition order avoids import/import deadlocks. No network I/O is
    # performed while locked, and the request is bounded to 2,000 source rows.
    if db.bind.dialect.name == "postgresql":
        await db.execute(text("SET LOCAL lock_timeout = '10s'"))
        await db.execute(text("SET LOCAL statement_timeout = '60s'"))
        await db.execute(text("LOCK TABLE media, shows, watch_events, comments, ratings, collections IN SHARE ROW EXCLUSIVE MODE"))


async def state_digest(db: AsyncSession, user_id: int) -> str:
    state = {}
    for model in (WatchEvent, Comment, Rating, Collection, SourceRecord, SourceRecordVersion, RatingRevision, RatingPolicy):
        rows = (await db.scalars(select(model).where(model.user_id == user_id).order_by(
            model.user_id if model is RatingPolicy else model.id))).all()
        state[model.__tablename__] = [row_dict(row) for row in rows]
    return digest(state)


async def source_record(db, user_id, manifest, record):
    return await db.scalar(select(SourceRecord).where(
        SourceRecord.user_id == user_id, SourceRecord.source == manifest.source,
        SourceRecord.account == manifest.account, SourceRecord.kind == record.kind,
        SourceRecord.external_id == record.external_id))


async def find_media(db, identity, *, create=False):
    clauses = []
    for field in ("tmdb_id", "tvdb_id", "imdb_id"):
        value = getattr(identity, field)
        if value:
            clauses.append(getattr(Media, field) == value)
    show = None
    if identity.type == "episode":
        show_clauses = []
        if identity.show_tmdb_id:
            show_clauses.append(Show.tmdb_id == identity.show_tmdb_id)
        if identity.show_tvdb_id:
            show_clauses.append(Show.tvdb_id == identity.show_tvdb_id)
        shows = (await db.scalars(select(Show).where(or_(*show_clauses)))).all()
        if len(shows) > 1:
            raise HTTPException(409, "Conflicting parent show identities")
        show = shows[0] if shows else None
        if show:
            for supplied, actual in ((identity.show_tmdb_id, show.tmdb_id), (identity.show_tvdb_id, show.tvdb_id)):
                if supplied and supplied != actual:
                    raise HTTPException(409, "Conflicting supplied parent show identity")
        if show:
            clauses.append((Media.show_id == show.id) & (Media.season_number == identity.season_number)
                           & (Media.episode_number == identity.episode_number))
    rows = (await db.scalars(select(Media).where(Media.media_type == identity.type, or_(*clauses)))).all() if clauses else []
    if len(rows) > 1:
        raise HTTPException(409, "Conflicting resolved media identities")
    if rows:
        media = rows[0]
        for field in ("tmdb_id", "tvdb_id", "imdb_id"):
            supplied, actual = getattr(identity, field), getattr(media, field)
            if supplied and actual and supplied != actual:
                raise HTTPException(409, "Media identity mismatch")
        if identity.type == "episode" and (
            show is None or media.show_id != show.id or media.season_number != identity.season_number
            or media.episode_number != identity.episode_number
        ):
            raise HTTPException(409, "Episode identity/numbering mismatch")
        return media
    if not create:
        return None
    if identity.type == "episode" and show is None:
        show = Show(tmdb_id=identity.show_tmdb_id, tvdb_id=identity.show_tvdb_id,
                    title=identity.show_title or identity.title,
                    canonical_source="tmdb" if identity.show_tmdb_id else "tvdb")
        db.add(show)
        await db.flush()
    media = Media(media_type=MediaType(identity.type), title=identity.title,
                  tmdb_id=identity.tmdb_id, tvdb_id=identity.tvdb_id, imdb_id=identity.imdb_id,
                  show_id=show.id if show else None, season_number=identity.season_number,
                  episode_number=identity.episode_number)
    db.add(media)
    await db.flush()
    return media


async def validate_matches(db, user_id, manifest):
    for record in manifest.records:
        media = await find_media(db, record.media)
        existing = await source_record(db, user_id, manifest, record)
        if existing and (media is None or existing.media_id != media.id):
            raise HTTPException(409, "Existing source identity cannot move to different media")
        if record.match_event_id:
            event = await db.scalar(select(WatchEvent).where(
                WatchEvent.id == record.match_event_id, WatchEvent.user_id == user_id))
            if event is None or media is None or event.media_id != media.id:
                raise HTTPException(409, "Matched event must belong to this user and exact media")
            if existing and existing.watch_event_id not in (None, event.id):
                raise HTTPException(409, "Existing source event cannot be relinked implicitly")
        if record.linked_event_external_id:
            matches = [r for r in manifest.records if r.external_id == record.linked_event_external_id
                       and r.kind == "watched_event"]
            prior = await db.scalar(select(SourceRecord).where(
                SourceRecord.user_id == user_id, SourceRecord.source == manifest.source,
                SourceRecord.account == manifest.account, SourceRecord.kind == "watched_event",
                SourceRecord.external_id == record.linked_event_external_id))
            if not matches and prior is None:
                raise HTTPException(409, "Linked review viewing has not been imported or included")
            if matches and matches[0].media != record.media:
                raise HTTPException(409, "Review and viewing media identities differ")
            if prior and (media is None or prior.media_id != media.id):
                raise HTTPException(409, "Review and existing viewing media differ")


async def preview(db: AsyncSession, user_id: int, manifest: ImportManifest):
    encoded = manifest.model_dump(mode="json")
    encoded["records"] = [record_evidence(record) for record in manifest.records]
    if len(json.dumps(encoded).encode()) > 20 * 1024 * 1024:
        raise HTTPException(413, "Manifest exceeds 20 MiB")
    await owner_lock(db, user_id)
    checksum = digest(encoded)
    existing = await db.scalar(select(ImportBatch).where(
        ImportBatch.user_id == user_id, ImportBatch.idempotency_key == manifest.idempotency_key))
    if existing and existing.manifest_sha256 != checksum:
        raise HTTPException(409, "Idempotency key already belongs to a different manifest")
    if existing and existing.status == "committed":
        return batch_summary(existing)
    await validate_matches(db, user_id, manifest)
    state = await state_digest(db, user_id)
    counts = Counter(record.kind for record in manifest.records)
    counts["unknown_dates"] = sum(r.occurred.precision == "unknown" for r in manifest.records)
    counts["date_only"] = sum(r.occurred.precision == "date" for r in manifest.records)
    counts["local_datetimes"] = sum(r.occurred.precision == "local_datetime" for r in manifest.records)
    counts["explicit_watch_links"] = sum(bool(r.match_event_id) for r in manifest.records)
    if existing:
        existing.state_sha256 = state
        existing.receipt = dict(counts)
        batch = existing
    else:
        batch = ImportBatch(user_id=user_id, idempotency_key=manifest.idempotency_key,
                            manifest_sha256=checksum, state_sha256=state, manifest=encoded,
                            status="preview", receipt=dict(counts))
        db.add(batch)
    await db.commit()
    return batch_summary(batch)


def batch_summary(batch):
    return {"batch_id": batch.id, "status": batch.status,
            "manifest_sha256": batch.manifest_sha256, "state_sha256": batch.state_sha256,
            "source": batch.manifest["source"], "account": batch.manifest["account"],
            "receipt": batch.receipt, "created_at": batch.created_at, "committed_at": batch.committed_at}


async def project_rating(db, user_id, media, season_number, episode_order):
    current = await db.scalar(select(Rating).where(Rating.user_id == user_id,
        Rating.media_id == media.id, Rating.season_number == season_number, Rating.episode_order == episode_order))
    revisions = (await db.scalars(select(RatingRevision).where(RatingRevision.user_id == user_id,
        RatingRevision.media_id == media.id, RatingRevision.season_number == season_number,
        RatingRevision.episode_order == episode_order).order_by(RatingRevision.id))).all()
    if current and current.selected_revision_id is None and current.rating is not None:
        legacy = RatingRevision(user_id=user_id, media_id=media.id, season_number=season_number,
            episode_order=episode_order, source="legacy", original_value=current.rating, original_scale=10,
            normalized_value=current.rating, occurred={"precision": "instant", "value": current.rated_at.isoformat() + "Z"}, review=current.review)
        db.add(legacy)
        await db.flush()
        current.selected_revision_id = legacy.id
        revisions.append(legacy)
    policy = await db.get(RatingPolicy, user_id)
    pool = revisions
    if policy and policy.movie_priority == "letterboxd" and media.media_type == MediaType.movie:
        preferred = [r for r in revisions if r.source == "letterboxd"]
        if preferred:
            pool = preferred
    selected = next((r for r in pool if current and r.id == current.selected_revision_id), None)
    conflicts = 0
    for revision in pool:
        if selected is None:
            selected = revision
            continue
        bounds, chosen_bounds = temporal_bounds(revision.occurred), temporal_bounds(selected.occurred)
        if bounds and (not chosen_bounds or bounds[0] > chosen_bounds[1]):
            selected = revision
        elif revision.id != selected.id and revision.normalized_value != selected.normalized_value:
            if not bounds or not chosen_bounds or not (bounds[1] < chosen_bounds[0]):
                conflicts += 1
    if selected:
        if current is None:
            current = Rating(user_id=user_id, media_id=media.id, season_number=season_number,
                             episode_order=episode_order)
            db.add(current)
        current.rating = float(selected.normalized_value)
        current.review = selected.review
        current.selected_revision_id = selected.id
        if selected.occurred.get("precision") == "instant":
            current.rated_at = temporal_columns(Occurred.model_validate(selected.occurred))["watched_at"]
        elif current.rated_at is None:
            # Compatibility column means insertion time only; selected revision is authoritative.
            current.rated_at = datetime.now(timezone.utc).replace(tzinfo=None)
    return conflicts


async def commit(db: AsyncSession, user_id: int, request: CommitRequest):
    await owner_lock(db, user_id)
    batch = await db.scalar(select(ImportBatch).where(ImportBatch.id == request.batch_id,
        ImportBatch.user_id == user_id).with_for_update())
    if batch is None:
        raise HTTPException(404, "Import batch not found")
    if request.manifest_sha256 != batch.manifest_sha256 or request.state_sha256 != batch.state_sha256:
        raise HTTPException(409, "Preview receipt mismatch")
    if batch.status == "committed":
        return batch_summary(batch)
    await import_write_lock(db)
    if await state_digest(db, user_id) != batch.state_sha256:
        raise HTTPException(409, "User history changed since preview; preview again")
    manifest = ImportManifest.model_validate(batch.manifest)
    await validate_matches(db, user_id, manifest)
    policy = await db.get(RatingPolicy, user_id)
    if policy is not None and policy.movie_priority != manifest.movie_rating_priority:
        # Changing policy requires reprojecting every prior rating, which is a
        # separate deliberate operation, not a side effect of an unrelated batch.
        raise HTTPException(409, "Manifest rating policy differs from established user policy")
    if policy is None:
        db.add(RatingPolicy(user_id=user_id, movie_priority=manifest.movie_rating_priority))
    else:
        policy.movie_priority = manifest.movie_rating_priority
    counts = Counter()
    rating_keys = {}
    # Reviews may refer to a viewing later in the uploaded source order.
    records = sorted(manifest.records, key=lambda r: r.kind == "review")
    for record in records:
        evidence = record_evidence(record)
        payload_hash = digest(evidence)
        source = await source_record(db, user_id, manifest, record)
        if source:
            version = await db.scalar(select(SourceRecordVersion).where(
                SourceRecordVersion.record_id == source.id, SourceRecordVersion.payload_sha256 == payload_hash))
            if version:
                counts["unchanged"] += 1
                continue
        media = await find_media(db, record.media, create=True)
        new_source = source is None
        if source is None:
            source = SourceRecord(user_id=user_id, source=manifest.source, account=manifest.account,
                kind=record.kind, external_id=record.external_id, media_id=media.id)
            db.add(source)
            await db.flush()
        version = SourceRecordVersion(user_id=user_id, record_id=source.id, batch_id=batch.id,
                                      payload_sha256=payload_hash, evidence=evidence)
        db.add(version)
        await db.flush()
        counts["source_versions"] += 1
        if record.kind.startswith("watched") and new_source:
            event = None
            if record.match_event_id:
                event = await db.get(WatchEvent, record.match_event_id)
            elif record.kind == "watched_assertion":
                # A status is evidence about media, not a guessed individual
                # viewing. Reviewed distinct occurrence identities may establish
                # a lower bound; existing real viewings already satisfy it.
                completed = await db.scalar(select(func.count()).select_from(WatchEvent).where(
                    WatchEvent.user_id == user_id, WatchEvent.media_id == media.id,
                    WatchEvent.completed.is_(True)))
                deficit = max(0, (record.minimum_known_viewings or 1) - completed)
                if counts["assertion_projections_created"] + deficit > 2000:
                    raise HTTPException(413, "Assertion projection count exceeds bounded batch limit")
                for _ in range(deficit):
                    db.add(WatchEvent(user_id=user_id, media_id=media.id, completed=True,
                        play_count=1, source=manifest.source, **temporal_columns(Occurred())))
                counts["assertion_projections_created"] += deficit
                counts["media_only_assertions"] += 1
                await db.flush()
                continue
            if event is None:
                event = WatchEvent(user_id=user_id, media_id=media.id, completed=True, play_count=1,
                    source=manifest.source, **temporal_columns(record.occurred))
                db.add(event)
                await db.flush()
                counts["watches_created"] += 1
            else:
                counts["watches_linked"] += 1
            source.watch_event_id = event.id
        elif record.kind.startswith("watched"):
            # Preserve revised source evidence without silently replacing canonical dates.
            counts["watch_revisions_for_review"] += 1
        elif record.kind == "review":
            if not new_source and source.comment_id is None:
                counts["deleted_reviews_preserved"] += 1
                continue
            linked = None
            if record.linked_event_external_id:
                linked = await db.scalar(select(SourceRecord).where(SourceRecord.user_id == user_id,
                    SourceRecord.source == manifest.source, SourceRecord.account == manifest.account,
                    SourceRecord.kind == "watched_event", SourceRecord.external_id == record.linked_event_external_id))
            comment = await db.get(Comment, source.comment_id) if source.comment_id else None
            if comment is None:
                comment = Comment(user_id=user_id, media_type=record.media.type,
                    tmdb_id=record.media.show_tmdb_id if media.media_type == MediaType.episode else media.tmdb_id,
                    tvdb_id=record.media.show_tvdb_id if media.media_type == MediaType.episode else media.tvdb_id,
                    season_number=media.season_number, episode_number=media.episode_number,
                    content=record.review, is_spoiler=record.spoiler)
                db.add(comment)
                await db.flush()
                source.comment_id = comment.id
                counts["reviews_created"] += 1
            comment.content = record.review
            comment.is_spoiler = record.spoiler
            comment.source_occurred = record.occurred.model_dump(mode="json")
            comment.watch_event_id = linked.watch_event_id if linked else None
        elif record.kind == "rating":
            score = record.rating
            db.add(RatingRevision(user_id=user_id, media_id=media.id, season_number=record.season_number,
                episode_order=record.episode_order, source_version_id=version.id, source=manifest.source,
                original_value=score.value, original_scale=score.scale,
                normalized_value=Decimal(10) * score.value / score.scale,
                occurred=record.occurred.model_dump(mode="json"), review=record.review))
            rating_keys[(media.id, record.season_number, record.episode_order)] = media
            counts["rating_revisions"] += 1
        elif record.kind == "watchlist":
            # Collection projection is additive and never removes the owner's existing list.
            existing = await db.scalar(select(Collection).where(Collection.user_id == user_id, Collection.media_id == media.id))
            if existing is None:
                db.add(Collection(user_id=user_id, media_id=media.id))
                counts["collection_added"] += 1
        await db.flush()
    for (media_id, season, order), media in rating_keys.items():
        counts["rating_conflicts"] += await project_rating(db, user_id, media, season, order)
    batch.status = "committed"
    batch.committed_at = datetime.now(timezone.utc)
    batch.receipt = dict(counts)
    await db.commit()
    return batch_summary(batch)


async def export_owner(db, user_id):
    # Authentication already used the request session; use a new read-only,
    # repeatable-read transaction so every table belongs to the same snapshot.
    async with db.bind.connect() as connection:
        await connection.execution_options(isolation_level="REPEATABLE READ")
        async with AsyncSession(bind=connection) as snapshot:
            async with snapshot.begin():
                await snapshot.execute(text("SET TRANSACTION READ ONLY"))
                return await _export_owner_snapshot(snapshot, user_id)


async def export_owner_file(db, user_id):
    """Write one repeatable-read snapshot without materializing its full JSON.

    The caller owns unlinking the private file after delivery. No SQL rows or
    temporary pathname are returned to an unauthenticated client.
    """
    descriptor, filename = tempfile.mkstemp(prefix="scrob-fidelity-", suffix=".json")
    destination = Path(filename)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            async with db.bind.connect() as connection:
                await connection.execution_options(isolation_level="REPEATABLE READ")
                async with AsyncSession(bind=connection) as snapshot:
                    async with snapshot.begin():
                        await snapshot.execute(text("SET TRANSACTION READ ONLY"))
                        media_ids = union(*(select(model.media_id).where(model.user_id == user_id)
                                            for model in (WatchEvent, Rating, SourceRecord)))
                        queries = [(model.__tablename__, select(model).where(model.user_id == user_id).order_by(
                            model.user_id if model is RatingPolicy else model.id))
                            for model in (ImportBatch, SourceRecord, SourceRecordVersion, RatingRevision,
                                          RatingPolicy, SourceArtifact, WatchEvent, Comment, Rating)]
                        queries.extend([
                            ("media", select(Media).where(Media.id.in_(media_ids)).order_by(Media.id)),
                            ("shows", select(Show).where(Show.id.in_(select(Media.show_id).where(
                                Media.id.in_(media_ids), Media.show_id.is_not(None)))).order_by(Show.id)),
                        ])
                        stream.write('{"schema_version":1,"format":"scrob-fidelity","user_id":')
                        json.dump(user_id, stream)
                        for name, query in queries:
                            stream.write("," + json.dumps(name) + ":[")
                            # Manifest, evidence and artifact rows contain arbitrary
                            # source text. Never prefetch many large blobs together.
                            batch_size = 1 if name in {
                                "fidelity_import_batches", "fidelity_source_versions", "fidelity_source_artifacts"
                            } else 100
                            rows = await snapshot.stream_scalars(query.execution_options(yield_per=batch_size))
                            try:
                                first = True
                                async for row in rows:
                                    if not first:
                                        stream.write(",")
                                    json.dump(row_dict(row), stream, ensure_ascii=False, separators=(",", ":"))
                                    first = False
                            finally:
                                await rows.close()
                            stream.write("]")
                        stream.write("}")
        return destination
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


async def _export_owner_snapshot(db, user_id):
    result = {"schema_version": 1, "format": "scrob-fidelity", "user_id": user_id}
    for model in (ImportBatch, SourceRecord, SourceRecordVersion, RatingRevision, RatingPolicy, SourceArtifact):
        rows = (await db.scalars(select(model).where(model.user_id == user_id).order_by(
            model.user_id if model is RatingPolicy else model.id))).all()
        result[model.__tablename__] = [row_dict(row) for row in rows]
    # Include the exact projections/links so an export is evidence-complete.
    for model in (WatchEvent, Comment, Rating):
        result[model.__tablename__] = [row_dict(row) for row in
            (await db.scalars(select(model).where(model.user_id == user_id).order_by(model.id))).all()]
    media_ids = {r["media_id"] for r in result["watch_events"] + result["ratings"] + result["fidelity_source_records"]}
    media = (await db.scalars(select(Media).where(Media.id.in_(media_ids)).order_by(Media.id))).all()
    result["media"] = [row_dict(row) for row in media]
    result["shows"] = [row_dict(row) for row in (await db.scalars(select(Show).where(
        Show.id.in_({row.show_id for row in media if row.show_id})).order_by(Show.id))).all()]
    return result

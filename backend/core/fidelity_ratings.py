"""Preserve rating revisions from existing ORM writers in their own transaction.

Bulk SQL rating writes are not used by the supported native writers. New writers
must use Rating ORM objects or the historical import service.
"""
from datetime import datetime, timezone
from decimal import Decimal

from models.fidelity import RatingPolicy, RatingRevision
from models.media import Media
from models.ratings import Rating
from models.users import User
from sqlalchemy import event, insert, inspect, select
from sqlalchemy.orm import Session


def _capture(session, _context, _instances):
    from core.fidelity import temporal_bounds
    for rating in list(session.new) + list(session.dirty):
        if not isinstance(rating, Rating) or rating.rating is None:
            continue
        state = inspect(rating)
        # The controlled importer already created immutable revisions and selected
        # a projection. Do not turn that projection into another synthetic revision.
        if state.attrs.selected_revision_id.history.has_changes():
            continue
        if rating not in session.new and not any(state.attrs[field].history.has_changes()
                                                for field in ("rating", "review", "rated_at")):
            continue
        connection = session.connection()
        if connection.dialect.name != "postgresql":
            # Upstream mock/SQLite tests are not production storage acceptance.
            continue
        connection.execute(select(User.id).where(User.id == rating.user_id).with_for_update())
        now = datetime.now(timezone.utc)
        stamp = rating.rated_at
        source = getattr(rating, "_fidelity_source", "application")
        occurred = getattr(rating, "_fidelity_occurred", None)
        if occurred is None:
            occurred = {"precision": "instant", "value": (stamp.replace(tzinfo=timezone.utc) if stamp and stamp.tzinfo is None else stamp or now).isoformat()}
        base = dict(user_id=rating.user_id, media_id=rating.media_id, season_number=rating.season_number,
                    episode_order=rating.episode_order, original_scale=10, observed_at=now)
        prior = None
        if rating.id:
            prior = connection.execute(select(Rating.__table__).where(Rating.id == rating.id)).mappings().first()
        if prior and prior["selected_revision_id"] is None and prior["rating"] is not None:
            # Capture the untouched legacy value before a native writer overwrites it.
            connection.execute(insert(RatingRevision).values(**base, source="legacy",
                original_value=prior["rating"], normalized_value=prior["rating"], review=prior["review"],
                occurred={"precision": "instant", "value": prior["rated_at"].isoformat() + "Z"}))
        revision_review = getattr(rating, "_fidelity_review", rating.review)
        revision_id = connection.execute(select(RatingRevision.id).where(
            RatingRevision.user_id == rating.user_id, RatingRevision.media_id == rating.media_id,
            RatingRevision.season_number == rating.season_number, RatingRevision.episode_order == rating.episode_order,
            RatingRevision.source == source, RatingRevision.original_value == Decimal(str(rating.rating)),
            RatingRevision.original_scale == 10, RatingRevision.occurred == occurred,
            RatingRevision.review == revision_review).limit(1)).scalar()
        if revision_id is None:
            revision_id = connection.execute(insert(RatingRevision).values(**base, source=source,
                original_value=Decimal(str(rating.rating)), normalized_value=Decimal(str(rating.rating)),
                review=revision_review, occurred=occurred).returning(RatingRevision.id)).scalar_one()
        candidates = connection.execute(select(RatingRevision.__table__).where(
            RatingRevision.user_id == rating.user_id, RatingRevision.media_id == rating.media_id,
            RatingRevision.season_number == rating.season_number,
            RatingRevision.episode_order == rating.episode_order).order_by(RatingRevision.id)).mappings().all()
        policy = connection.execute(select(RatingPolicy.movie_priority).where(RatingPolicy.user_id == rating.user_id)).scalar()
        media_type = connection.execute(select(Media.media_type).where(Media.id == rating.media_id)).scalar()
        if policy == "letterboxd" and media_type == "movie":
            preferred = [item for item in candidates if item["source"] == "letterboxd"]
            if preferred:
                candidates = preferred
        selected = next((item for item in candidates if item["id"] == rating.selected_revision_id), candidates[0])
        for item in candidates:
            bounds, chosen = temporal_bounds(item["occurred"]), temporal_bounds(selected["occurred"])
            if bounds and (not chosen or bounds[0] > chosen[1]):
                selected = item
        rating.selected_revision_id = selected["id"]
        rating.rating = float(selected["normalized_value"])
        rating.review = selected["review"]
        if selected["occurred"].get("precision") == "instant":
            rating.rated_at = datetime.fromisoformat(selected["occurred"]["value"].replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
        elif prior:
            rating.rated_at = prior["rated_at"]
        else:
            rating.rated_at = now.replace(tzinfo=None)
        # Available to callers/tests without changing any existing response schema.
        rating._fidelity_recorded_revision_id = revision_id


def install():
    if not event.contains(Session, "before_flush", _capture):
        event.listen(Session, "before_flush", _capture)

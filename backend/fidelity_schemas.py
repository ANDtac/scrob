"""Versioned, source-neutral historical evidence contract. Never contains credentials."""
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Occurred(StrictModel):
    precision: Literal["instant", "date", "local_datetime", "unknown"] = "unknown"
    value: str | None = None
    timezone: str | None = None
    raw: str | None = None

    @model_validator(mode="after")
    def valid_time(self):
        if self.timezone:
            try:
                ZoneInfo(self.timezone)
            except ZoneInfoNotFoundError as exc:
                raise ValueError("unknown IANA timezone") from exc
        if self.precision == "unknown":
            if self.value is not None:
                raise ValueError("unknown time cannot contain a normalized value")
        elif self.precision == "date":
            if not self.value or len(self.value) != 10:
                raise ValueError("date requires YYYY-MM-DD")
            date.fromisoformat(self.value)
        else:
            if not self.value or "T" not in self.value:
                raise ValueError("datetime requires ISO date and time")
            parsed = datetime.fromisoformat(self.value.replace("Z", "+00:00"))
            if (parsed.tzinfo is not None) != (self.precision == "instant"):
                raise ValueError("instant requires offset; local_datetime must have no offset")
        return self


class MediaIdentity(StrictModel):
    type: Literal["movie", "series", "episode"]
    tmdb_id: int | None = Field(None, gt=0)
    tvdb_id: int | None = Field(None, gt=0)
    imdb_id: str | None = Field(None, pattern=r"^tt\d+$")
    title: str = Field(min_length=1, max_length=500)
    # Episode IDs above are episode IDs, never series IDs in disguise.
    show_tmdb_id: int | None = Field(None, gt=0)
    show_tvdb_id: int | None = Field(None, gt=0)
    show_title: str | None = Field(None, max_length=500)
    season_number: int | None = Field(None, ge=0)
    episode_number: int | None = Field(None, ge=0)

    @model_validator(mode="after")
    def valid_identity(self):
        if self.type == "episode":
            if not (self.show_tmdb_id or self.show_tvdb_id):
                raise ValueError("episode requires independently resolved show identity")
            if self.season_number is None or self.episode_number is None:
                raise ValueError("episode requires canonical season and episode")
        elif not (self.tmdb_id or self.tvdb_id or self.imdb_id):
            raise ValueError("media requires an external identity")
        return self


class Score(StrictModel):
    value: Decimal = Field(ge=0)
    scale: Decimal = Field(gt=0)

    @model_validator(mode="after")
    def valid_score(self):
        if not self.value.is_finite() or not self.scale.is_finite() or self.value > self.scale:
            raise ValueError("rating must be finite and within its original scale")
        return self


class EvidenceRecord(StrictModel):
    external_id: str = Field(min_length=1, max_length=512)
    kind: Literal["watched_event", "watched_assertion", "rating", "review", "watchlist"]
    media: MediaIdentity
    occurred: Occurred = Field(default_factory=Occurred)
    payload: dict[str, Any] = Field(default_factory=dict)
    locator: str | None = Field(None, max_length=2048)
    source_sha256: str | None = Field(None, pattern=r"^[0-9a-f]{64}$")
    match_event_id: int | None = Field(None, gt=0)
    linked_event_external_id: str | None = Field(None, max_length=512)
    review: str | None = Field(None, max_length=1_000_000)
    spoiler: bool = False
    rating: Score | None = None
    season_number: int | None = Field(None, ge=0)
    episode_order: str | None = Field(None, max_length=40)
    minimum_known_viewings: int | None = Field(None, ge=1, le=100, strict=True)

    @model_validator(mode="after")
    def valid_record(self):
        if self.kind == "rating" and self.rating is None:
            raise ValueError("rating record requires rating")
        if self.kind == "review" and self.review is None:
            raise ValueError("review record requires review text")
        if self.kind.startswith("watched") and self.media.type == "series":
            raise ValueError("series watched assertions must be resolved to episodes")
        if self.match_event_id and not self.kind.startswith("watched"):
            raise ValueError("match_event_id is only valid on watched evidence")
        if self.minimum_known_viewings is not None:
            if self.kind != "watched_assertion" or self.match_event_id is not None:
                raise ValueError("minimum count requires media-only watched assertion")
            proof = self.payload.get("minimum_viewings_proof", {})
            ids = proof.get("distinct_occurrence_ids", [])
            if (proof.get("reviewed") is not True or not isinstance(ids, list)
                    or len(ids) != self.minimum_known_viewings
                    or any(not isinstance(value, str) or not value.strip() for value in ids)
                    or len(set(ids)) != len(ids)):
                raise ValueError("minimum count requires reviewed distinct occurrence identities")
        return self


class ImportManifest(StrictModel):
    schema_version: Literal[1] = 1
    source: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,15}$")
    account: str = Field(min_length=1, max_length=255)
    idempotency_key: str = Field(min_length=1, max_length=128)
    parser_version: str = Field(min_length=1, max_length=128)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    # An explicit per-user choice, never inferred from a hard-coded username.
    movie_rating_priority: Literal["letterboxd", "newest_dated"] = "newest_dated"
    records: list[EvidenceRecord] = Field(min_length=1, max_length=2000)

    @model_validator(mode="after")
    def unique_ids(self):
        keys = [(r.kind, r.external_id) for r in self.records]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate source identities in manifest")
        return self


class CommitRequest(StrictModel):
    batch_id: int = Field(gt=0)
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class SourceArtifactUpload(StrictModel):
    source: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,15}$")
    account: str = Field(min_length=1, max_length=255)
    filename: str = Field(min_length=1, max_length=255, pattern=r"^[^/\\]+$")
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    media_type: Literal["application/json", "text/csv", "text/plain"]
    # Original UTF-8 bytes, retained verbatim including line endings.
    content: str = Field(max_length=20_000_000)


class StatusReconciliation(StrictModel):
    """Explicit legacy status projections to preserve as non-viewing evidence."""
    idempotency_key: str = Field(min_length=1, max_length=128)
    account: str = Field(min_length=1, max_length=255)
    trakt_account: str = Field(min_length=1, max_length=255)
    artifact_id: int = Field(gt=0)
    artifact_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_ids: list[int] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_events(self):
        if any(event <= 0 for event in self.event_ids) or len(set(self.event_ids)) != len(self.event_ids):
            raise ValueError("event IDs must be unique positive integers")
        return self

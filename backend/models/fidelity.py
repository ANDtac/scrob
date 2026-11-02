"""Additive owner-scoped evidence; raw revisions are append-only in the import service."""
from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ImportBatch(Base):
    __tablename__ = "fidelity_import_batches"
    __table_args__ = (
        UniqueConstraint("user_id", "idempotency_key", name="uq_fidelity_batch_key"),
        UniqueConstraint("id", "user_id", name="uq_fidelity_batch_owner"),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(128))
    manifest_sha256: Mapped[str] = mapped_column(String(64))
    state_sha256: Mapped[str] = mapped_column(String(64))
    manifest: Mapped[dict] = mapped_column(JSONB)
    status: Mapped[str] = mapped_column(String(16), default="preview")
    receipt: Mapped[dict] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    committed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SourceRecord(Base):
    __tablename__ = "fidelity_source_records"
    __table_args__ = (
        UniqueConstraint("user_id", "source", "account", "kind", "external_id", name="uq_fidelity_source_identity"),
        UniqueConstraint("id", "user_id", name="uq_fidelity_source_owner"),
        ForeignKeyConstraint(["watch_event_id", "user_id"], ["watch_events.id", "watch_events.user_id"]),
        ForeignKeyConstraint(["comment_id", "user_id"], ["comments.id", "comments.user_id"]),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    source: Mapped[str] = mapped_column(String(16))
    account: Mapped[str] = mapped_column(String(255))
    kind: Mapped[str] = mapped_column(String(32))
    external_id: Mapped[str] = mapped_column(String(512))
    media_id: Mapped[int] = mapped_column(ForeignKey("media.id"), index=True)
    watch_event_id: Mapped[int | None] = mapped_column(Integer, index=True)
    comment_id: Mapped[int | None] = mapped_column(Integer, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SourceRecordVersion(Base):
    __tablename__ = "fidelity_source_versions"
    __table_args__ = (
        UniqueConstraint("record_id", "payload_sha256", name="uq_fidelity_version_hash"),
        UniqueConstraint("id", "user_id", name="uq_fidelity_version_owner"),
        ForeignKeyConstraint(["record_id", "user_id"], ["fidelity_source_records.id", "fidelity_source_records.user_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(["batch_id", "user_id"], ["fidelity_import_batches.id", "fidelity_import_batches.user_id"]),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    record_id: Mapped[int] = mapped_column(Integer, index=True)
    batch_id: Mapped[int] = mapped_column(Integer, index=True)
    payload_sha256: Mapped[str] = mapped_column(String(64))
    evidence: Mapped[dict] = mapped_column(JSONB)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RatingRevision(Base):
    __tablename__ = "fidelity_rating_revisions"
    __table_args__ = (
        UniqueConstraint("id", "user_id", name="uq_fidelity_rating_owner"),
        UniqueConstraint("source_version_id", name="uq_fidelity_rating_version"),
        ForeignKeyConstraint(["source_version_id", "user_id"], ["fidelity_source_versions.id", "fidelity_source_versions.user_id"]),
    )
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    media_id: Mapped[int] = mapped_column(ForeignKey("media.id"), index=True)
    season_number: Mapped[int | None] = mapped_column(Integer)
    episode_order: Mapped[str | None] = mapped_column(String(40))
    source_version_id: Mapped[int | None] = mapped_column(Integer, index=True)
    source: Mapped[str] = mapped_column(String(16))
    original_value: Mapped[float] = mapped_column(Numeric())
    original_scale: Mapped[float] = mapped_column(Numeric())
    normalized_value: Mapped[float] = mapped_column(Numeric())
    occurred: Mapped[dict] = mapped_column(JSONB)
    review: Mapped[str | None] = mapped_column(Text)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class RatingPolicy(Base):
    __tablename__ = "fidelity_rating_policies"
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    movie_priority: Mapped[str] = mapped_column(String(24), default="newest_dated")


class SourceArtifact(Base):
    __tablename__ = "fidelity_source_artifacts"
    __table_args__ = (UniqueConstraint("user_id", "source", "account", "sha256", name="uq_fidelity_artifact_hash"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    source: Mapped[str] = mapped_column(String(16))
    account: Mapped[str] = mapped_column(String(255))
    filename: Mapped[str] = mapped_column(String(255))
    sha256: Mapped[str] = mapped_column(String(64))
    media_type: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

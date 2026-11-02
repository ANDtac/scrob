"""Add owner-scoped historical evidence without rewriting existing timestamps."""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "hfidelity1"
down_revision = "savedflt"
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint("uq_watch_event_owner", "watch_events", ["id", "user_id"])
    op.create_unique_constraint("uq_comment_owner", "comments", ["id", "user_id"])
    op.add_column("watch_events", sa.Column("watched_precision", sa.String(20), nullable=False, server_default="legacy"))
    op.add_column("watch_events", sa.Column("watched_on", sa.Date()))
    op.add_column("watch_events", sa.Column("watched_local_at", sa.DateTime()))
    op.add_column("watch_events", sa.Column("watched_timezone", sa.String(100)))
    op.add_column("comments", sa.Column("watch_event_id", sa.Integer()))
    op.add_column("comments", sa.Column("source_occurred", JSONB()))
    op.create_index("ix_comments_watch_event_id", "comments", ["watch_event_id"])
    op.create_foreign_key("fk_comment_watch_owner", "comments", "watch_events", ["watch_event_id", "user_id"], ["id", "user_id"])
    # Freeze the initial table definitions in this migration. Future changes use
    # a new migration; do not import the current ORM schema during upgrades.
    op.create_table("fidelity_import_batches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("idempotency_key", sa.String(128), nullable=False),
        sa.Column("manifest_sha256", sa.String(64), nullable=False),
        sa.Column("state_sha256", sa.String(64), nullable=False),
        sa.Column("manifest", JSONB(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("receipt", JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("committed_at", sa.DateTime(timezone=True)),
        sa.UniqueConstraint("user_id", "idempotency_key", name="uq_fidelity_batch_key"),
        sa.UniqueConstraint("id", "user_id", name="uq_fidelity_batch_owner"))
    op.create_table("fidelity_source_records",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.String(16), nullable=False), sa.Column("account", sa.String(255), nullable=False),
        sa.Column("kind", sa.String(32), nullable=False), sa.Column("external_id", sa.String(512), nullable=False),
        sa.Column("media_id", sa.Integer(), sa.ForeignKey("media.id"), nullable=False),
        sa.Column("watch_event_id", sa.Integer()), sa.Column("comment_id", sa.Integer()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "source", "account", "kind", "external_id", name="uq_fidelity_source_identity"),
        sa.UniqueConstraint("id", "user_id", name="uq_fidelity_source_owner"),
        sa.ForeignKeyConstraint(["watch_event_id", "user_id"], ["watch_events.id", "watch_events.user_id"]),
        sa.ForeignKeyConstraint(["comment_id", "user_id"], ["comments.id", "comments.user_id"]))
    op.create_table("fidelity_source_versions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False), sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("payload_sha256", sa.String(64), nullable=False), sa.Column("evidence", JSONB(), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("record_id", "payload_sha256", name="uq_fidelity_version_hash"),
        sa.UniqueConstraint("id", "user_id", name="uq_fidelity_version_owner"),
        sa.ForeignKeyConstraint(["record_id", "user_id"], ["fidelity_source_records.id", "fidelity_source_records.user_id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["batch_id", "user_id"], ["fidelity_import_batches.id", "fidelity_import_batches.user_id"]))
    op.create_table("fidelity_rating_revisions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("media_id", sa.Integer(), sa.ForeignKey("media.id"), nullable=False),
        sa.Column("season_number", sa.Integer()), sa.Column("episode_order", sa.String(40)),
        sa.Column("source_version_id", sa.Integer()), sa.Column("source", sa.String(16), nullable=False),
        sa.Column("original_value", sa.Numeric(12, 6), nullable=False), sa.Column("original_scale", sa.Numeric(12, 6), nullable=False),
        sa.Column("normalized_value", sa.Numeric(12, 6), nullable=False), sa.Column("occurred", JSONB(), nullable=False),
        sa.Column("review", sa.Text()),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("id", "user_id", name="uq_fidelity_rating_owner"),
        sa.UniqueConstraint("source_version_id", name="uq_fidelity_rating_version"),
        sa.ForeignKeyConstraint(["source_version_id", "user_id"], ["fidelity_source_versions.id", "fidelity_source_versions.user_id"]))
    op.create_table("fidelity_rating_policies",
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), primary_key=True),
        sa.Column("movie_priority", sa.String(24), nullable=False))
    for table, columns in {
        "fidelity_import_batches": ["user_id"],
        "fidelity_source_records": ["user_id", "media_id", "watch_event_id", "comment_id"],
        "fidelity_source_versions": ["user_id", "record_id", "batch_id"],
        "fidelity_rating_revisions": ["user_id", "media_id", "source_version_id"],
    }.items():
        for column in columns:
            op.create_index(f"ix_{table}_{column}", table, [column])
    op.add_column("ratings", sa.Column("selected_revision_id", sa.Integer()))
    op.create_index("ix_ratings_selected_revision_id", "ratings", ["selected_revision_id"])
    op.create_foreign_key("fk_rating_selected_owner", "ratings", "fidelity_rating_revisions", ["selected_revision_id", "user_id"], ["id", "user_id"])


def downgrade():
    raise RuntimeError("Restore a verified pre-upgrade database and original image; downgrade would discard evidence.")

"""Retain original source files and unlink deleted projections without losing evidence."""
import sqlalchemy as sa
from alembic import op

revision = "hfidelity2"
down_revision = "hfidelity1"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table("fidelity_source_artifacts",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("user_id", sa.Integer(), sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("source", sa.String(16), nullable=False), sa.Column("account", sa.String(255), nullable=False),
        sa.Column("filename", sa.String(255), nullable=False), sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("media_type", sa.String(32), nullable=False), sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint("user_id", "source", "account", "sha256", name="uq_fidelity_artifact_hash"))
    op.create_index("ix_fidelity_source_artifacts_user_id", "fidelity_source_artifacts", ["user_id"])
    op.execute("""CREATE FUNCTION fidelity_unlink_watch() RETURNS trigger LANGUAGE plpgsql
        SET search_path = pg_catalog, public AS $$ BEGIN
          UPDATE public.fidelity_source_records SET watch_event_id = NULL WHERE watch_event_id = OLD.id AND user_id = OLD.user_id;
          UPDATE public.comments SET watch_event_id = NULL WHERE watch_event_id = OLD.id AND user_id = OLD.user_id;
          RETURN OLD; END $$""")
    op.execute("CREATE TRIGGER fidelity_unlink_watch BEFORE DELETE ON watch_events FOR EACH ROW EXECUTE FUNCTION fidelity_unlink_watch()")
    op.execute("""CREATE FUNCTION fidelity_unlink_comment() RETURNS trigger LANGUAGE plpgsql
        SET search_path = pg_catalog, public AS $$ BEGIN
          UPDATE public.fidelity_source_records SET comment_id = NULL WHERE comment_id = OLD.id AND user_id = OLD.user_id;
          RETURN OLD; END $$""")
    op.execute("CREATE TRIGGER fidelity_unlink_comment BEFORE DELETE ON comments FOR EACH ROW EXECUTE FUNCTION fidelity_unlink_comment()")
    op.create_check_constraint("ck_watch_precision", "watch_events", "watched_precision IN ('legacy','instant','date','local_datetime','unknown')")
    for column in ("original_value", "original_scale", "normalized_value"):
        op.alter_column("fidelity_rating_revisions", column, type_=sa.Numeric())


def downgrade():
    raise RuntimeError("Restore the verified pre-upgrade database and image; do not discard evidence.")

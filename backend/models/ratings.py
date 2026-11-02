from datetime import datetime
from typing import Optional, TYPE_CHECKING

from sqlalchemy import DateTime, Float, ForeignKey, ForeignKeyConstraint, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from .base import Base
if TYPE_CHECKING:
    from .users import User
    from .media import Media


RatingKey = tuple[int, int | None]
RatingChanges = dict[RatingKey, float]


class Rating(Base):
    __tablename__ = "ratings"
    __table_args__ = (ForeignKeyConstraint(["selected_revision_id", "user_id"], ["fidelity_rating_revisions.id", "fidelity_rating_revisions.user_id"], use_alter=True, name="fk_rating_selected_owner"),)

    id            : Mapped[int]             = mapped_column(Integer, primary_key=True)
    user_id       : Mapped[int]             = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    media_id      : Mapped[int]             = mapped_column(ForeignKey("media.id", ondelete="CASCADE"), nullable=False)
    season_number : Mapped[Optional[int]]   = mapped_column(Integer, nullable=True)
    episode_order : Mapped[Optional[str]]   = mapped_column(String(40), nullable=True)
    rating        : Mapped[Optional[float]] = mapped_column(Float)
    review        : Mapped[Optional[str]]   = mapped_column(Text)
    rated_at      : Mapped[datetime]        = mapped_column(DateTime, server_default=func.now(), nullable=False)
    selected_revision_id: Mapped[Optional[int]] = mapped_column(Integer, index=True)

    # Unique constraint is a COALESCE expression index (see migration); no SQLAlchemy UniqueConstraint here.

    user  : Mapped["User"]  = relationship(back_populates="ratings")
    media : Mapped["Media"] = relationship(back_populates="ratings")

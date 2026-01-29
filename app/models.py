from datetime import datetime
from sqlalchemy import (
    String,
    Float,
    Integer,
    DateTime,
    ForeignKey,
    Index,
    Text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .db import Base


class Event(Base):
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)

    user_id: Mapped[str] = mapped_column(String(64), index=True)
    merchant_id: Mapped[str] = mapped_column(String(64), index=True)

    amount: Mapped[float] = mapped_column(Float)
    currency: Mapped[str] = mapped_column(String(8))

    timestamp: Mapped[datetime] = mapped_column(DateTime, index=True)

    lat: Mapped[float] = mapped_column(Float)
    lon: Mapped[float] = mapped_column(Float)

    device_id: Mapped[str] = mapped_column(String(128), index=True)
    ip: Mapped[str] = mapped_column(String(64), index=True)
    channel: Mapped[str] = mapped_column(String(32))

    features: Mapped["FeatureRow"] = relationship(back_populates="event", uselist=False)
    score: Mapped["ScoreRow"] = relationship(back_populates="event", uselist=False)


class FeatureRow(Base):
    __tablename__ = "features"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("events.id"), unique=True, index=True
    )

    log_amount: Mapped[float] = mapped_column(Float)

    tx_count_5m: Mapped[int] = mapped_column(Integer)
    tx_count_1h: Mapped[int] = mapped_column(Integer)
    spend_1h: Mapped[float] = mapped_column(Float)

    is_new_merchant: Mapped[int] = mapped_column(Integer)
    is_new_device: Mapped[int] = mapped_column(Integer)
    is_new_ip: Mapped[int] = mapped_column(Integer)

    distance_from_last_km: Mapped[float] = mapped_column(Float)
    speed_kmph: Mapped[float] = mapped_column(Float)

    hour_of_day: Mapped[int] = mapped_column(Integer)
    day_of_week: Mapped[int] = mapped_column(Integer)

    event: Mapped[Event] = relationship(back_populates="features")


class ScoreRow(Base):
    __tablename__ = "scores"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_id_fk: Mapped[int] = mapped_column(
        Integer, ForeignKey("events.id"), unique=True, index=True
    )

    anomaly_score: Mapped[float] = mapped_column(Float)
    risk_score: Mapped[float] = mapped_column(Float)
    flagged: Mapped[int] = mapped_column(Integer)

    reasons_json: Mapped[str] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime, index=True)

    event: Mapped[Event] = relationship(back_populates="score")


class ModelRun(Base):
    __tablename__ = "model_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, index=True)

    model_type: Mapped[str] = mapped_column(String(64))
    feature_list_json: Mapped[str] = mapped_column(Text)
    threshold_json: Mapped[str] = mapped_column(Text)


Index("idx_events_user_time", Event.user_id, Event.timestamp)

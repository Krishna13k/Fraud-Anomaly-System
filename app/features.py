from __future__ import annotations
from dataclasses import dataclass
from datetime import timedelta
from math import radians, sin, cos, asin, sqrt, log
from sqlalchemy.orm import Session
from .models import Event


@dataclass
class ComputedFeatures:
    log_amount: float
    tx_count_5m: int
    tx_count_1h: int
    spend_1h: float
    is_new_merchant: int
    is_new_device: int
    is_new_ip: int
    distance_from_last_km: float
    speed_kmph: float
    hour_of_day: int
    day_of_week: int


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    return r * c


def compute_features(db: Session, incoming: Event) -> ComputedFeatures:
    t = incoming.timestamp

    window_5m_start = t - timedelta(minutes=5)
    window_1h_start = t - timedelta(hours=1)

    recent_5m = (
        db.query(Event)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.timestamp >= window_5m_start)
        .filter(Event.timestamp < t)
        .all()
    )

    recent_1h = (
        db.query(Event)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.timestamp >= window_1h_start)
        .filter(Event.timestamp < t)
        .all()
    )

    tx_count_5m = len(recent_5m)
    tx_count_1h = len(recent_1h)

    spend_1h = 0.0
    for e in recent_1h:
        spend_1h += float(e.amount)

    last_event = (
        db.query(Event)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.timestamp < t)
        .order_by(Event.timestamp.desc())
        .first()
    )

    is_new_merchant = 1
    if (
        db.query(Event.id)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.merchant_id == incoming.merchant_id)
        .filter(Event.timestamp < t)
        .first()
        is not None
    ):
        is_new_merchant = 0

    is_new_device = 1
    if (
        db.query(Event.id)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.device_id == incoming.device_id)
        .filter(Event.timestamp < t)
        .first()
        is not None
    ):
        is_new_device = 0

    is_new_ip = 1
    if (
        db.query(Event.id)
        .filter(Event.user_id == incoming.user_id)
        .filter(Event.ip == incoming.ip)
        .filter(Event.timestamp < t)
        .first()
        is not None
    ):
        is_new_ip = 0

    distance_from_last_km = 0.0
    speed_kmph = 0.0
    if last_event is not None:
        distance_from_last_km = haversine_km(last_event.lat, last_event.lon, incoming.lat, incoming.lon)
        dt_seconds = (incoming.timestamp - last_event.timestamp).total_seconds()
        if dt_seconds > 0:
            speed_kmph = distance_from_last_km / (dt_seconds / 3600.0)

    amt = float(incoming.amount)
    if amt > 0:
        log_amount = log(amt)
    else:
        log_amount = 0.0

    hour_of_day = incoming.timestamp.hour
    day_of_week = incoming.timestamp.weekday()

    return ComputedFeatures(
        log_amount=log_amount,
        tx_count_5m=tx_count_5m,
        tx_count_1h=tx_count_1h,
        spend_1h=spend_1h,
        is_new_merchant=is_new_merchant,
        is_new_device=is_new_device,
        is_new_ip=is_new_ip,
        distance_from_last_km=distance_from_last_km,
        speed_kmph=speed_kmph,
        hour_of_day=hour_of_day,
        day_of_week=day_of_week,
    )
from datetime import datetime
import json
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from dateutil.parser import isoparse
from sqlalchemy.orm import Session
from sqlalchemy import desc

from .db import SessionLocal, engine, Base
from .models import Event, FeatureRow, ScoreRow, ModelRun
from .features import compute_features
from .scoring import score_feature_vector, feature_columns, reset_cache
from .reasons import build_reasons, reasons_to_json

app = FastAPI(title="Fraud / Anomaly Detection System")

Base.metadata.create_all(bind=engine)


class IngestEventRequest(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)
    user_id: str = Field(min_length=1, max_length=64)
    merchant_id: str = Field(min_length=1, max_length=64)
    amount: float
    currency: str = Field(min_length=1, max_length=8)
    timestamp: str
    lat: float
    lon: float
    device_id: str = Field(min_length=1, max_length=128)
    ip: str = Field(min_length=1, max_length=64)
    channel: str = Field(min_length=1, max_length=32)


class IngestEventResponse(BaseModel):
    stored_event_id: int
    stored_feature_id: int


class EventWithFeaturesResponse(BaseModel):
    event_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    timestamp: str
    lat: float
    lon: float
    device_id: str
    ip: str
    channel: str

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


class ScoreResponse(BaseModel):
    event_id: str
    anomaly_score: float
    risk_score: float
    flagged: bool
    reasons: List[dict]


class RetrainResponse(BaseModel):
    trained_rows: int
    threshold: float
    percentile: float


class ScoreQueueItem(BaseModel):
    event_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    timestamp: str
    device_id: str
    ip: str
    channel: str

    anomaly_score: float
    risk_score: float
    flagged: bool
    reasons: List[dict]
    scored_at_utc: str


class ModelRunItem(BaseModel):
    created_at_utc: str
    model_type: str
    trained_rows: int
    threshold: float
    percentile: float
    feature_columns: List[str]


class ScoreByEventIdRequest(BaseModel):
    event_id: str = Field(min_length=1, max_length=64)


def get_db() -> Session:
    return SessionLocal()


@app.get("/health")
def health():
    return {"status": "ok"}


def upsert_event_and_features(db: Session, req: IngestEventRequest):
    existing = db.query(Event).filter(Event.event_id == req.event_id).first()
    if existing is not None:
        fr = db.query(FeatureRow).filter(FeatureRow.event_id_fk == existing.id).first()
        if fr is None:
            raise HTTPException(status_code=500, detail="event exists but features missing")
        return existing, fr

    try:
        ts = isoparse(req.timestamp)
        if ts.tzinfo is not None:
            ts = ts.astimezone(tz=None).replace(tzinfo=None)
    except Exception:
        raise HTTPException(status_code=400, detail="invalid timestamp format")

    ev = Event(
        event_id=req.event_id,
        user_id=req.user_id,
        merchant_id=req.merchant_id,
        amount=float(req.amount),
        currency=req.currency,
        timestamp=ts,
        lat=float(req.lat),
        lon=float(req.lon),
        device_id=req.device_id,
        ip=req.ip,
        channel=req.channel,
    )

    db.add(ev)
    db.commit()
    db.refresh(ev)

    feats = compute_features(db, ev)

    fr = FeatureRow(
        event_id_fk=ev.id,
        log_amount=feats.log_amount,
        tx_count_5m=feats.tx_count_5m,
        tx_count_1h=feats.tx_count_1h,
        spend_1h=feats.spend_1h,
        is_new_merchant=feats.is_new_merchant,
        is_new_device=feats.is_new_device,
        is_new_ip=feats.is_new_ip,
        distance_from_last_km=feats.distance_from_last_km,
        speed_kmph=feats.speed_kmph,
        hour_of_day=feats.hour_of_day,
        day_of_week=feats.day_of_week,
    )

    db.add(fr)
    db.commit()
    db.refresh(fr)

    return ev, fr


def upsert_score_row(db: Session, ev: Event, anomaly_score: float, risk_score: float, flagged: bool, reasons: list[dict]):
    reasons_json = reasons_to_json(reasons)

    existing_score = db.query(ScoreRow).filter(ScoreRow.event_id_fk == ev.id).first()
    if existing_score is None:
        sr = ScoreRow(
            event_id_fk=ev.id,
            anomaly_score=float(anomaly_score),
            risk_score=float(risk_score),
            flagged=1 if flagged else 0,
            reasons_json=reasons_json,
            created_at=datetime.utcnow(),
        )
        db.add(sr)
    else:
        existing_score.anomaly_score = float(anomaly_score)
        existing_score.risk_score = float(risk_score)
        existing_score.flagged = 1 if flagged else 0
        existing_score.reasons_json = reasons_json
        existing_score.created_at = datetime.utcnow()

    db.commit()


@app.post("/ingest", response_model=IngestEventResponse)
def ingest(req: IngestEventRequest):
    db = get_db()
    try:
        ev, fr = upsert_event_and_features(db, req)
        return IngestEventResponse(stored_event_id=ev.id, stored_feature_id=fr.id)
    finally:
        db.close()


@app.get("/events", response_model=List[EventWithFeaturesResponse])
def list_events(limit: int = 100, user_id: Optional[str] = None):
    db = get_db()
    try:
        q = (
            db.query(Event, FeatureRow)
            .join(FeatureRow, FeatureRow.event_id_fk == Event.id)
            .order_by(desc(Event.timestamp))
        )

        if user_id is not None:
            q = q.filter(Event.user_id == user_id)

        rows = q.limit(limit).all()
        out: List[EventWithFeaturesResponse] = []

        for ev, fr in rows:
            out.append(
                EventWithFeaturesResponse(
                    event_id=ev.event_id,
                    user_id=ev.user_id,
                    merchant_id=ev.merchant_id,
                    amount=float(ev.amount),
                    currency=ev.currency,
                    timestamp=ev.timestamp.isoformat(),
                    lat=float(ev.lat),
                    lon=float(ev.lon),
                    device_id=ev.device_id,
                    ip=ev.ip,
                    channel=ev.channel,
                    log_amount=float(fr.log_amount),
                    tx_count_5m=int(fr.tx_count_5m),
                    tx_count_1h=int(fr.tx_count_1h),
                    spend_1h=float(fr.spend_1h),
                    is_new_merchant=int(fr.is_new_merchant),
                    is_new_device=int(fr.is_new_device),
                    is_new_ip=int(fr.is_new_ip),
                    distance_from_last_km=float(fr.distance_from_last_km),
                    speed_kmph=float(fr.speed_kmph),
                    hour_of_day=int(fr.hour_of_day),
                    day_of_week=int(fr.day_of_week),
                )
            )

        return out
    finally:
        db.close()


@app.post("/score", response_model=ScoreResponse)
def score(req: IngestEventRequest):
    db = get_db()
    try:
        ev, fr = upsert_event_and_features(db, req)

        cols = feature_columns()
        feature_values = [float(getattr(fr, c)) for c in cols]

        anomaly_score, risk_score, flagged = score_feature_vector(feature_values)

        reasons = build_reasons(db, ev, fr)
        upsert_score_row(db, ev, anomaly_score, risk_score, flagged, reasons)

        return ScoreResponse(
            event_id=ev.event_id,
            anomaly_score=float(anomaly_score),
            risk_score=float(risk_score),
            flagged=bool(flagged),
            reasons=reasons,
        )
    finally:
        db.close()


@app.post("/score-by-event-id", response_model=ScoreResponse)
def score_by_event_id(req: ScoreByEventIdRequest):
    db = get_db()
    try:
        ev = db.query(Event).filter(Event.event_id == req.event_id).first()
        if ev is None:
            raise HTTPException(status_code=404, detail="event_id not found")

        fr = db.query(FeatureRow).filter(FeatureRow.event_id_fk == ev.id).first()
        if fr is None:
            raise HTTPException(status_code=404, detail="features not found for event_id")

        cols = feature_columns()
        feature_values = [float(getattr(fr, c)) for c in cols]

        anomaly_score, risk_score, flagged = score_feature_vector(feature_values)

        reasons = build_reasons(db, ev, fr)
        upsert_score_row(db, ev, anomaly_score, risk_score, flagged, reasons)

        return ScoreResponse(
            event_id=ev.event_id,
            anomaly_score=float(anomaly_score),
            risk_score=float(risk_score),
            flagged=bool(flagged),
            reasons=reasons,
        )
    finally:
        db.close()


@app.post("/retrain", response_model=RetrainResponse)
def retrain(percentile: float = 99.0):
    from training.train import fit_and_save

    info = fit_and_save(percentile=float(percentile))
    reset_cache()

    db = get_db()
    try:
        mr = ModelRun(
            created_at=datetime.utcnow(),
            model_type="IsolationForest",
            feature_list_json=json.dumps(info["feature_columns"]),
            threshold_json=json.dumps(
                {
                    "threshold": info["threshold"],
                    "percentile": info["percentile"],
                    "trained_rows": info["trained_rows"],
                }
            ),
        )
        db.add(mr)
        db.commit()
    finally:
        db.close()

    return RetrainResponse(
        trained_rows=int(info["trained_rows"]),
        threshold=float(info["threshold"]),
        percentile=float(info["percentile"]),
    )


@app.get("/scores", response_model=List[ScoreQueueItem])
def list_scores(
    limit: int = 100,
    flagged_only: bool = True,
    min_risk: float = 0.0,
    user_id: Optional[str] = None,
):
    db = get_db()
    try:
        q = (
            db.query(Event, ScoreRow)
            .join(ScoreRow, ScoreRow.event_id_fk == Event.id)
            .order_by(desc(ScoreRow.created_at))
        )

        if flagged_only:
            q = q.filter(ScoreRow.flagged == 1)

        if min_risk > 0:
            q = q.filter(ScoreRow.risk_score >= float(min_risk))

        if user_id is not None:
            q = q.filter(Event.user_id == user_id)

        rows = q.limit(limit).all()

        out: List[ScoreQueueItem] = []
        for ev, sr in rows:
            try:
                reasons = json.loads(sr.reasons_json) if sr.reasons_json else []
            except Exception:
                reasons = []

            out.append(
                ScoreQueueItem(
                    event_id=ev.event_id,
                    user_id=ev.user_id,
                    merchant_id=ev.merchant_id,
                    amount=float(ev.amount),
                    currency=ev.currency,
                    timestamp=ev.timestamp.isoformat(),
                    device_id=ev.device_id,
                    ip=ev.ip,
                    channel=ev.channel,
                    anomaly_score=float(sr.anomaly_score),
                    risk_score=float(sr.risk_score),
                    flagged=bool(sr.flagged == 1),
                    reasons=reasons,
                    scored_at_utc=sr.created_at.isoformat(),
                )
            )

        return out
    finally:
        db.close()


@app.get("/model-runs", response_model=List[ModelRunItem])
def list_model_runs(limit: int = 50):
    db = get_db()
    try:
        runs = db.query(ModelRun).order_by(desc(ModelRun.created_at)).limit(limit).all()
        current_feature_count = int(db.query(FeatureRow).count())

        out: List[ModelRunItem] = []
        for r in runs:
            feature_cols: List[str] = []
            threshold = 0.0
            percentile = 0.0
            trained_rows = None

            try:
                feature_cols = json.loads(r.feature_list_json) if r.feature_list_json else []
            except Exception:
                feature_cols = []

            try:
                t = json.loads(r.threshold_json) if r.threshold_json else {}
                if "threshold" in t:
                    threshold = float(t["threshold"])
                if "percentile" in t:
                    percentile = float(t["percentile"])
                if "trained_rows" in t:
                    trained_rows = int(t["trained_rows"])
            except Exception:
                pass

            if trained_rows is None:
                trained_rows = current_feature_count

            out.append(
                ModelRunItem(
                    created_at_utc=r.created_at.isoformat(),
                    model_type=r.model_type,
                    trained_rows=int(trained_rows),
                    threshold=float(threshold),
                    percentile=float(percentile),
                    feature_columns=list(feature_cols),
                )
            )

        return out
    finally:
        db.close()
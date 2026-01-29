import json
import os
from datetime import datetime
import joblib
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine

DB_URL = "sqlite:///./fraud.db"

FEATURE_COLUMNS = [
    "log_amount",
    "tx_count_5m",
    "tx_count_1h",
    "spend_1h",
    "is_new_merchant",
    "is_new_device",
    "is_new_ip",
    "distance_from_last_km",
    "speed_kmph",
    "hour_of_day",
    "day_of_week",
]

ARTIFACT_DIR = "artifacts"
MODEL_PATH = os.path.join(ARTIFACT_DIR, "model.joblib")
SCALER_PATH = os.path.join(ARTIFACT_DIR, "scaler.joblib")
THRESH_PATH = os.path.join(ARTIFACT_DIR, "threshold.json")


def load_features() -> pd.DataFrame:
    engine = create_engine(DB_URL)
    df = pd.read_sql_query("SELECT * FROM features", engine)
    return df


def fit_and_save(percentile: float = 99.0) -> dict:
    os.makedirs(ARTIFACT_DIR, exist_ok=True)

    df = load_features()
    if df.shape[0] < 50:
        raise RuntimeError(f"Need at least 50 feature rows to train, found {df.shape[0]}")

    X = df[FEATURE_COLUMNS].astype(float).values

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    model = IsolationForest(
        n_estimators=300,
        random_state=42,
        n_jobs=-1,
        contamination="auto",
    )
    model.fit(Xs)

    anomaly_scores = -model.score_samples(Xs)
    threshold = float(pd.Series(anomaly_scores).quantile(percentile / 100.0))

    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)

    payload = {
        "threshold": threshold,
        "percentile": float(percentile),
        "trained_rows": int(df.shape[0]),
        "trained_at_utc": datetime.utcnow().isoformat(),
        "feature_columns": FEATURE_COLUMNS,
        "score_definition": "anomaly_score = -model.score_samples(StandardScaled(X))",
    }

    with open(THRESH_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    return payload


if __name__ == "__main__":
    info = fit_and_save(percentile=99.0)
    print(json.dumps(info, indent=2))

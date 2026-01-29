import json
import os
import joblib
import numpy as np

ARTIFACT_DIR = "artifacts"
MODEL_PATH = os.path.join(ARTIFACT_DIR, "model.joblib")
SCALER_PATH = os.path.join(ARTIFACT_DIR, "scaler.joblib")
THRESH_PATH = os.path.join(ARTIFACT_DIR, "threshold.json")

_cached = {"model": None, "scaler": None, "threshold": None, "feature_columns": None}


def load_artifacts():
    if _cached["model"] is not None:
        return

    if not (os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH) and os.path.exists(THRESH_PATH)):
        raise RuntimeError("Model artifacts missing. Run: python training\\train.py")

    _cached["model"] = joblib.load(MODEL_PATH)
    _cached["scaler"] = joblib.load(SCALER_PATH)

    with open(THRESH_PATH, "r", encoding="utf-8") as f:
        t = json.load(f)

    _cached["threshold"] = float(t["threshold"])
    _cached["feature_columns"] = list(t["feature_columns"])


def feature_columns():
    load_artifacts()
    return list(_cached["feature_columns"])


def score_feature_vector(feature_values):
    load_artifacts()

    X = np.array(feature_values, dtype=float).reshape(1, -1)
    Xs = _cached["scaler"].transform(X)

    anomaly_score = float(-_cached["model"].score_samples(Xs)[0])
    threshold = float(_cached["threshold"])
    flagged = anomaly_score >= threshold

    if threshold <= 0:
        risk = 0.0
    else:
        risk = 100.0 * anomaly_score / threshold
        if risk < 0:
            risk = 0.0
        if risk > 100:
            risk = 100.0

    return anomaly_score, float(risk), bool(flagged)


def reset_cache():
    _cached["model"] = None
    _cached["scaler"] = None
    _cached["threshold"] = None
    _cached["feature_columns"] = None

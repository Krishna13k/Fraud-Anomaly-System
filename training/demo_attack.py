import random
import time
from datetime import datetime, timedelta, timezone
import requests

BASE_URL = "http://127.0.0.1:8000"
RETRAIN_PERCENTILE = 98.0

HOME_CITY = ("Chicago", 41.8781, -87.6298)
FRAUD_CITY = ("Los Angeles", 34.0522, -118.2437)

MERCHANTS = ["m_amazon", "m_target", "m_walmart", "m_shell", "m_costco", "m_uber"]
CHANNELS = ["mobile", "web"]

SESSION = requests.Session()


def post_json(path: str, payload: dict, timeout: int = 30, retries: int = 6):
    url = f"{BASE_URL}{path}"
    last_err = None

    for attempt in range(retries):
        try:
            r = SESSION.post(url, json=payload, timeout=timeout)
            if r.status_code >= 300:
                raise RuntimeError(f"{path} failed {r.status_code}: {r.text}")
            return r.json()
        except Exception as e:
            last_err = e
            backoff = 0.25 * (2 ** attempt)
            time.sleep(backoff)

    raise RuntimeError(f"{path} failed after retries: {last_err}")


def post_no_body(path: str, timeout: int = 60, retries: int = 6):
    url = f"{BASE_URL}{path}"
    last_err = None

    for attempt in range(retries):
        try:
            r = SESSION.post(url, timeout=timeout)
            if r.status_code >= 300:
                raise RuntimeError(f"{path} failed {r.status_code}: {r.text}")
            return r.json()
        except Exception as e:
            last_err = e
            backoff = 0.25 * (2 ** attempt)
            time.sleep(backoff)

    raise RuntimeError(f"{path} failed after retries: {last_err}")


def get_json(path: str, timeout: int = 30, retries: int = 6):
    url = f"{BASE_URL}{path}"
    last_err = None

    for attempt in range(retries):
        try:
            r = SESSION.get(url, timeout=timeout)
            if r.status_code >= 300:
                raise RuntimeError(f"{path} failed {r.status_code}: {r.text}")
            return r.json()
        except Exception as e:
            last_err = e
            backoff = 0.25 * (2 ** attempt)
            time.sleep(backoff)

    raise RuntimeError(f"{path} failed after retries: {last_err}")


def retrain():
    return post_no_body(f"/retrain?percentile={RETRAIN_PERCENTILE}", timeout=120, retries=8)


def score_event(payload: dict):
    return post_json("/score", payload, timeout=30, retries=8)


def make_event(event_id: str, user_id: str, merchant_id: str, amount: float, ts: datetime, lat: float, lon: float, device_id: str, ip: str, channel: str):
    return {
        "event_id": event_id,
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": float(round(amount, 2)),
        "currency": "USD",
        "timestamp": ts.isoformat().replace("+00:00", "Z"),
        "lat": float(lat),
        "lon": float(lon),
        "device_id": device_id,
        "ip": ip,
        "channel": channel,
    }


def run_demo():
    print("1) Retraining model for demo (slightly more sensitive threshold)...")
    info = retrain()
    print(info)

    user_id = "demo_user_attack"
    base_time = datetime.now(timezone.utc) - timedelta(hours=2)

    _, home_lat, home_lon = HOME_CITY
    _, fraud_lat, fraud_lon = FRAUD_CITY

    normal_device = "demo_device_normal"
    normal_ip = "11.11.11.11"

    print("\n2) Sending NORMAL baseline events...")
    ts = base_time
    for i in range(30):
        ts = ts + timedelta(minutes=random.randint(8, 25))
        amount = random.uniform(8, 80)
        merchant = random.choice(MERCHANTS)
        channel = random.choice(CHANNELS)

        payload = make_event(
            event_id=f"{user_id}_normal_{i}",
            user_id=user_id,
            merchant_id=merchant,
            amount=amount,
            ts=ts,
            lat=home_lat + random.uniform(-0.01, 0.01),
            lon=home_lon + random.uniform(-0.01, 0.01),
            device_id=normal_device,
            ip=normal_ip,
            channel=channel,
        )

        score_event(payload)
        time.sleep(0.03)

    print("Baseline complete.")

    print("\n3) Launching FRAUD burst (new device/ip + geo jump + velocity + high amounts)...")
    fraud_device = "demo_device_fraud"
    fraud_ip = "99.99.99.99"

    burst_start = ts + timedelta(minutes=3)
    results = []
    for j in range(10):
        t = burst_start + timedelta(seconds=15 * j)
        amount = random.uniform(250, 2000)

        payload = make_event(
            event_id=f"{user_id}_fraud_{j}",
            user_id=user_id,
            merchant_id=f"m_fraud_{j}",
            amount=amount,
            ts=t,
            lat=fraud_lat + random.uniform(-0.02, 0.02),
            lon=fraud_lon + random.uniform(-0.02, 0.02),
            device_id=fraud_device,
            ip=fraud_ip,
            channel="web",
        )

        out = score_event(payload)
        results.append(out)
        time.sleep(0.03)

    print("\n4) Fraud burst results:")
    flagged_count = 0
    for r in results:
        if r.get("flagged") is True:
            flagged_count += 1
        reasons = [x.get("reason") for x in r.get("reasons", [])]
        print(f"- event_id={r['event_id']} flagged={r['flagged']} risk={r['risk_score']:.2f} reasons={reasons}")

    print(f"\nFlagged in burst: {flagged_count}/{len(results)}")

    print("\n5) Pulling review queue (scores)...")
    q = get_json("/scores?limit=10&flagged_only=true", timeout=30, retries=8)
    print(q)


if __name__ == "__main__":
    run_demo()
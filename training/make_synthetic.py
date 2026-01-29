import random
from datetime import datetime, timedelta, timezone
from faker import Faker
import requests

fake = Faker()

API_URL = "http://127.0.0.1:8000/ingest"

US_CITIES = [
    ("Chicago", 41.8781, -87.6298),
    ("New York", 40.7128, -74.0060),
    ("Los Angeles", 34.0522, -118.2437),
    ("Miami", 25.7617, -80.1918),
    ("Dallas", 32.7767, -96.7970),
    ("Seattle", 47.6062, -122.3321),
]

MERCHANTS = ["m_amazon", "m_target", "m_uber", "m_walmart", "m_shell", "m_apple", "m_costco", "m_bestbuy"]
CHANNELS = ["web", "mobile"]
CURRENCIES = ["USD"]


def pick_home_city():
    return random.choice(US_CITIES)


def clamp_amount(x: float) -> float:
    if x < 1.0:
        return 1.0
    return float(x)


def post_event(payload: dict) -> None:
    r = requests.post(API_URL, json=payload, timeout=10)
    if r.status_code >= 300:
        raise RuntimeError(f"ingest failed {r.status_code}: {r.text}")


def make_normal_event(user_id: str, base_time: datetime, home_city: tuple, device_id: str, ip: str, event_num: int):
    city_name, lat, lon = home_city

    amount = random.lognormvariate(3.3, 0.55)
    amount = clamp_amount(amount)

    merchant_id = random.choice(MERCHANTS)
    channel = random.choice(CHANNELS)

    payload = {
        "event_id": f"{user_id}_evt_{event_num}",
        "user_id": user_id,
        "merchant_id": merchant_id,
        "amount": round(amount, 2),
        "currency": "USD",
        "timestamp": base_time.isoformat().replace("+00:00", "Z"),
        "lat": lat + random.uniform(-0.01, 0.01),
        "lon": lon + random.uniform(-0.01, 0.01),
        "device_id": device_id,
        "ip": ip,
        "channel": channel,
    }
    return payload


def make_fraud_burst(user_id: str, start_time: datetime, home_city: tuple, burst_id: int):
    fraud_city = random.choice([c for c in US_CITIES if c[0] != home_city[0]])
    _, home_lat, home_lon = home_city
    _, fraud_lat, fraud_lon = fraud_city

    device_id = f"fraud_device_{burst_id}_{fake.uuid4()[:8]}"
    ip = fake.ipv4_public()

    events = []
    for i in range(6):
        t = start_time + timedelta(seconds=20 * i)
        amount = random.uniform(150, 1200)

        payload = {
            "event_id": f"{user_id}_fraud_{burst_id}_{i}",
            "user_id": user_id,
            "merchant_id": f"m_fraud_{random.randint(1, 20)}",
            "amount": round(amount, 2),
            "currency": "USD",
            "timestamp": t.isoformat().replace("+00:00", "Z"),
            "lat": fraud_lat + random.uniform(-0.02, 0.02),
            "lon": fraud_lon + random.uniform(-0.02, 0.02),
            "device_id": device_id,
            "ip": ip,
            "channel": "web",
        }
        events.append(payload)

    return events


def generate(user_count: int = 15, normal_events_per_user: int = 40, fraud_users: int = 4):
    start = datetime.now(timezone.utc) - timedelta(days=2)

    users = []
    for i in range(user_count):
        user_id = f"user_{i+1}"
        home_city = pick_home_city()
        device_id = f"dev_{i+1}_{fake.uuid4()[:6]}"
        ip = fake.ipv4_public()
        users.append((user_id, home_city, device_id, ip))

    fraud_user_ids = set([u[0] for u in random.sample(users, fraud_users)])

    event_num = 1
    for (user_id, home_city, device_id, ip) in users:
        t = start + timedelta(minutes=random.randint(0, 120))

        for j in range(normal_events_per_user):
            t = t + timedelta(minutes=random.randint(10, 120))
            payload = make_normal_event(user_id, t, home_city, device_id, ip, event_num)
            post_event(payload)
            event_num += 1

        if user_id in fraud_user_ids:
            burst_time = t + timedelta(minutes=5)
            burst_events = make_fraud_burst(user_id, burst_time, home_city, burst_id=random.randint(1, 9999))
            for p in burst_events:
                post_event(p)

    print("Synthetic ingestion complete.")
    print(f"Users: {user_count}, normal/user: {normal_events_per_user}, fraud_users: {fraud_users}")


if __name__ == "__main__":
    generate()
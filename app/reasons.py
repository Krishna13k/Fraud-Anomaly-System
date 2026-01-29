import json
from sqlalchemy.orm import Session
from .models import Event, FeatureRow


def build_reasons(db: Session, ev: Event, fr: FeatureRow):
    reasons = []

    if int(fr.is_new_device) == 1:
        reasons.append({"reason": "new_device", "detail": "Device not seen before for this user", "severity": 85})

    if int(fr.is_new_ip) == 1:
        reasons.append({"reason": "new_ip", "detail": "IP address not seen before for this user", "severity": 80})

    if int(fr.is_new_merchant) == 1:
        reasons.append({"reason": "new_merchant", "detail": "Merchant not seen before for this user", "severity": 65})

    if float(fr.speed_kmph) >= 900:
        reasons.append({"reason": "impossible_travel", "detail": f"Travel speed {float(fr.speed_kmph):.0f} km/h", "severity": 95})

    if int(fr.tx_count_5m) >= 3:
        reasons.append({"reason": "high_velocity_5m", "detail": f"{int(fr.tx_count_5m)} transactions in 5 minutes", "severity": 90})

    if float(fr.spend_1h) >= 800:
        reasons.append({"reason": "high_spend_1h", "detail": f"${float(fr.spend_1h):.2f} spend in last hour", "severity": 70})

    prior_amounts = (
        db.query(Event.amount)
        .filter(Event.user_id == ev.user_id)
        .filter(Event.timestamp < ev.timestamp)
        .order_by(Event.timestamp.desc())
        .limit(30)
        .all()
    )

    if len(prior_amounts) >= 10:
        vals = [float(x[0]) for x in prior_amounts]
        vals_sorted = sorted(vals)
        median = vals_sorted[len(vals_sorted) // 2]
        if median > 0 and float(ev.amount) >= 4.0 * median:
            reasons.append(
                {"reason": "amount_spike", "detail": f"Amount ${float(ev.amount):.2f} is >= 4x user median ${median:.2f}", "severity": 75}
            )

    reasons_sorted = sorted(reasons, key=lambda r: int(r["severity"]), reverse=True)
    return reasons_sorted[:3]


def reasons_to_json(reasons):
    return json.dumps(reasons, ensure_ascii=False)
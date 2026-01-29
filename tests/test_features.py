from app.features import haversine_km


def test_haversine_zero():
    d = haversine_km(41.0, -87.0, 41.0, -87.0)
    assert abs(d - 0.0) < 1e-9


def test_haversine_known_direction():
    d1 = haversine_km(41.8781, -87.6298, 34.0522, -118.2437)
    d2 = haversine_km(34.0522, -118.2437, 41.8781, -87.6298)
    assert d1 > 1000
    assert abs(d1 - d2) < 1e-6

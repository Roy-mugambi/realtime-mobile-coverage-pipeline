"""
SYNTHETIC DATA GENERATOR — Generates realistic mobile network logs

WHAT THIS DOES:
  Generates a rich, realistic mobile-network-coverage CSV. 
  The data has STRUCTURE that mirrors real telecom behaviour, to help our dashboard reveal
  genuine patterns.

SCHEMA:
 COLUMNS = {hour,lat,long,signal,network,operator,status,description,speed,
  satellites,precission,provider,activity,postal_code}

STRUCTURAL BEHAVIOR OF OUR OUTPUT FILE:
    - Different operators have different baseline signal quality
    - Signal degrades during busy hours (network congestion)
    - GPS precision is worse for "gps" provider than "fused"
    - GPS degrades when moving fast (in vehicles) vs stationary
    - Most events are "in service", but outages cluster in time + place
    - Signal varies by geography (urban vs rural postal codes)"""

 
import csv
import random
import math
from datetime import datetime, timedelta

random.seed(1807)

OUTPUT_PATH = "mobile-logs1.csv"
TOTAL_RECORDS = 250


OPERATORS = [
    ("movistar",  "Movistar",      32, 8,  0.02),   # premium, reliable
    ("vodafone",  "vodafone ES",   30, 9,  0.03),   # premium
    ("orange",    "Orange",        28, 10, 0.04),   # good
    ("orange",    "JAZZTEL",       22, 11, 0.06),   # budget (Orange MVNO)
    ("yoigo",     "Yoigo",         20, 12, 0.07),   # budget
    ("movistar",  "Tuenti",        21, 11, 0.06),   # budget (Movistar MVNO)
    ("vodafone",  "pepephone",     19, 12, 0.08),   # budget MVNO
    ("orange",    "simyo",         18, 13, 0.09),   # budget MVNO
    ("yoigo",     "MASMOVIL",      17, 13, 0.10),   # weakest coverage
]


ACTIVITIES = [
    ("STILL",       (0.0, 0.5),   0),    # best GPS
    ("ON_FOOT",     (0.5, 2.0),   5),
    ("ON_BICYCLE",  (2.0, 8.0),  10),
    ("IN_VEHICLE",  (8.0, 35.0), 20),    # worst GPS (fast movement)
    ("TILTING",     (0.0, 1.0),   3),
    ("UNKNOWN",     (0.0, 5.0),   8),
]


IN_SERVICE = ("0", "STATE_IN_SERVICE")
OUTAGE_STATES = [
    ("1", "STATE_OUT_OF_SERVICE"),
    ("2", "STATE_EMERGENCY_ONLY"),
    ("3", "STATE_POWER_OFF"),
]


POSTAL_AREAS = [
    ("28013", 40.4168, -3.7038,  5),   # Madrid centre (urban)
    ("08002", 41.3851,  2.1734,  5),   # Barcelona centre (urban)
    ("41001", 37.3891, -5.9845,  4),   # Seville (urban)
    ("46002", 39.4699, -0.3763,  4),   # Valencia (urban)
    ("48001", 43.2630, -2.9350,  3),   # Bilbao
    ("29001", 36.7213, -4.4214,  3),   # Malaga
    ("15001", 43.3623, -8.4115,  1),   # A Coruna
    ("47001", 41.6523, -4.7245,  0),   # Valladolid
    ("06001", 38.8794, -6.9707, -2),   # Badajoz (rural-ish)
    ("22001", 42.1401, -0.4089, -3),   # Huesca (rural)
    ("44001", 40.3456, -1.1065, -4),   # Teruel (rural, poor coverage)
    ("05001", 40.6566, -4.6818, -3),   # Avila (rural)
]

PROVIDERS = ["gps", "fused"]


def hourly_volume_weight(hour: int) -> float:
    """
    Returns a relative volume multiplier for a given hour of day.

    Models real mobile usage: low overnight (2-5am), ramping through
    morning, peak midday and evening. This makes the time-series charts
    show a natural daily rhythm instead of flat uniform volume.
    """
    # A simple bimodal curve: morning peak ~9am, evening peak ~8pm
    morning = math.exp(-((hour - 9) ** 2) / 8)
    evening = math.exp(-((hour - 20) ** 2) / 10)
    overnight_floor = 0.1
    return overnight_floor + morning + evening


def congestion_penalty(hour: int) -> float:
    """
    Signal degrades during peak hours due to network congestion.
    Returns a signal penalty (subtracted from baseline).
    Peak congestion around 8-10am and 7-10pm.
    """
    morning = math.exp(-((hour - 9) ** 2) / 6) * 6
    evening = math.exp(-((hour - 20) ** 2) / 8) * 8
    return morning + evening


def generate() -> None:

    weights = [hourly_volume_weight(h) for h in range(24)]
    total_weight = sum(weights)
    records_per_hour = [
        max(1, round(TOTAL_RECORDS * w / total_weight)) for w in weights
    ]

    rows = []

    for hour in range(24):
        n = records_per_hour[hour]
        congestion = congestion_penalty(hour)

        for _ in range(n):
            # pick an operator ---
            network, operator, base_sig, spread, outage_rate = random.choice(OPERATORS)

            # pick a geographic area ---
            postal, base_lat, base_lon, urban_bonus = random.choice(POSTAL_AREAS)
            # jitter the coordinates slightly around the area centre
            lat = round(base_lat + random.uniform(-0.05, 0.05), 5)
            lon = round(base_lon + random.uniform(-0.05, 0.05), 5)

            # pick an activity ---
            activity, speed_range, gps_penalty = random.choice(ACTIVITIES)
            speed = round(random.uniform(*speed_range), 1)

            # compute signal ---
            # baseline + urban bonus - congestion - random noise, clamped 0-65
            signal = base_sig + urban_bonus - congestion + random.gauss(0, spread)
            signal = int(max(0, min(65, round(signal))))

            # determine status
            # outage chance rises when signal is very low
            effective_outage = outage_rate + (0.15 if signal < 5 else 0)
            if random.random() < effective_outage:
                status, description = random.choice(OUTAGE_STATES)
            else:
                status, description = IN_SERVICE

            # GPS provider + precision + satellites
            provider = random.choices(PROVIDERS, weights=[0.7, 0.3])[0]
            # fused is more accurate (lower precision number) than raw gps
            base_precision = 8 if provider == "fused" else 18
            precision = base_precision + gps_penalty + random.uniform(0, 15)
            precision = round(precision, 1)
            # satellites: fewer when moving fast / poor conditions
            base_sats = 9 if provider == "fused" else 6
            satellites = max(0, round(base_sats - gps_penalty / 6 + random.gauss(0, 1.5)))
            satellites = float(satellites)

            
            minute = random.randint(0, 59)
            second = random.randint(0, 59)
            hhmmss = f"{hour:02d}:{minute:02d}:{second:02d}"

            rows.append({
                "hour":        hhmmss,
                "lat":         lat,
                "long":        lon,
                "signal":      signal,
                "network":     network,
                "operator":    operator,
                "status":      status,
                "description": description,
                "speed":       speed,
                "satellites":  satellites,
                "precission":  precision,   
                "provider":    provider,
                "activity":    activity,
                "postal_code": postal,
            })

  
    rows.sort(key=lambda r: r["hour"])


    fieldnames = [
        "hour", "lat", "long", "signal", "network", "operator", "status",
        "description", "speed", "satellites", "precission", "provider",
        "activity", "postal_code",
    ]
    with open(OUTPUT_PATH, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

 
    from collections import Counter
    print(f"Generated {len(rows):,} records → {OUTPUT_PATH}")
    print()
    print("Hours covered:", len(set(r['hour'][:2] for r in rows)), "of 24")
    print("Operators:", len(set(r['operator'] for r in rows)))
    print("Postal areas:", len(set(r['postal_code'] for r in rows)))
    print()
    sig_by_op = {}
    for r in rows:
        sig_by_op.setdefault(r['operator'], []).append(r['signal'])
    print("Avg signal by operator (should vary — premium higher):")
    for op in sorted(sig_by_op, key=lambda o: -sum(sig_by_op[o])/len(sig_by_op[o])):
        vals = sig_by_op[op]
        print(f"  {op:<14} {sum(vals)/len(vals):5.1f}  (n={len(vals)})")
    print()
    statuses = Counter(r['description'] for r in rows)
    print("Status distribution:")
    for desc, cnt in statuses.most_common():
        print(f"  {desc:<24} {cnt:>5} ({100*cnt/len(rows):.1f}%)")


if __name__ == "__main__":
    generate()
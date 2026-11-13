"""
SYNTHETIC DATA GENERATOR: Kenyan mobile network coverage logs

WHAT THIS DOES:
  Generates a synthetic mobile-network-coverage CSV modelled on the Kenyan
  market: Kenyan operators, Kenyan towns and their postal codes, and
  realistic daily usage and congestion patterns. It is NOT real operator
  data; every value is generated with a fixed seed so runs are reproducible.

SCHEMA (unchanged from v1, so Spark, dbt and Snowflake need no changes):
  hour, lat, long, signal, network, operator, status, description, speed,
  satellites, precission, provider, activity, postal_code
  ("precission" keeps the original source typo on purpose; Spark renames it.)

STRUCTURE BAKED INTO THE DATA:
  - Operator mix weighted roughly like Kenyan subscription shares (approximate,
    an assumption for synthetic data, not official figures)
  - Coverage quality differs by operator and by town (urban vs remote)
  - Signal degrades during morning and evening peaks (congestion)
  - Volume follows a daily rhythm: quiet overnight, busy mornings and evenings
  - Bigger towns produce more readings (traffic weighted by area)
  - Outages are likelier where signal is weak
  - GPS precision is worse for raw "gps" than "fused", and worse when moving

USAGE:
  python producer/generator.py                 # 20,000 rows → data/kenya-mobile-logs.csv
  python producer/generator.py --rows 50000
"""

import argparse
import csv
import math
import random
from collections import Counter
from pathlib import Path

SEED = 1807
DEFAULT_ROWS = 20_000
DEFAULT_OUTPUT = "data/kenya-mobile-logs.csv"

# (network, operator, base_signal, spread, outage_rate, market_weight)
# network = who owns the radio network; operator = the brand on the SIM.
# Equitel is an MVNO riding on Airtel; Faiba runs on JTL's network.
OPERATORS = [
    ("safaricom", "Safaricom", 34,  8, 0.02, 0.62),
    ("airtel",    "Airtel",    28, 10, 0.04, 0.29),
    ("telkom",    "Telkom",    22, 11, 0.07, 0.04),
    ("airtel",    "Equitel",   26, 10, 0.05, 0.03),
    ("jtl",       "Faiba",     20, 12, 0.08, 0.02),
]

# (postal_code, town, lat, long, urban_bonus, traffic_weight)
# Codes keep their leading zeros (00100, 01000): a deliberate test of every
# layer that might treat them as numbers.
POSTAL_AREAS = [
    ("00100", "Nairobi GPO", -1.2864, 36.8172,  6, 30),
    ("80100", "Mombasa",     -4.0435, 39.6682,  5, 10),
    ("40100", "Kisumu",      -0.0917, 34.7680,  4,  7),
    ("20100", "Nakuru",      -0.3031, 36.0800,  4,  7),
    ("30100", "Eldoret",      0.5143, 35.2698,  3,  6),
    ("01000", "Thika",       -1.0333, 37.0693,  3,  5),
    ("90100", "Machakos",    -1.5177, 37.2634,  2,  4),
    ("10100", "Nyeri",       -0.4201, 36.9476,  2,  4),
    ("10400", "Nanyuki",      0.0074, 37.0722,  1,  3),
    ("60200", "Meru",         0.0463, 37.6559,  1,  4),
    ("50100", "Kakamega",     0.2827, 34.7519,  0,  4),
    ("40200", "Kisii",       -0.6817, 34.7667,  0,  4),
    ("80200", "Malindi",     -3.2192, 40.1169, -1,  2),
    ("30200", "Kitale",       1.0157, 35.0062, -1,  3),
    ("70100", "Garissa",     -0.4532, 39.6461, -4,  2),
    ("60300", "Isiolo",       0.3546, 37.5822, -3,  2),
    ("30500", "Lodwar",       3.1191, 35.5973, -6,  1),
]

ACTIVITIES = [
    # (activity, speed range m/s, gps_penalty, weight)
    ("STILL",      (0.0, 0.5),   0, 30),
    ("ON_FOOT",    (0.5, 2.0),   5, 25),
    ("IN_VEHICLE", (8.0, 30.0), 20, 25),   # matatus, boda bodas, private cars
    ("ON_BICYCLE", (2.0, 8.0),  10,  5),
    ("TILTING",    (0.0, 1.0),   3,  5),
    ("UNKNOWN",    (0.0, 5.0),   8, 10),
]

IN_SERVICE = ("0", "STATE_IN_SERVICE")
OUTAGE_STATES = [
    ("1", "STATE_OUT_OF_SERVICE"),
    ("2", "STATE_EMERGENCY_ONLY"),
    ("3", "STATE_POWER_OFF"),
]
PROVIDERS = ["gps", "fused"]

FIELDNAMES = [
    "hour", "lat", "long", "signal", "network", "operator", "status",
    "description", "speed", "satellites", "precission", "provider",
    "activity", "postal_code",
]


def hourly_volume_weight(hour: int) -> float:
    """Low overnight, a morning peak around 08:00 and an evening peak around 19:00."""
    morning = math.exp(-((hour - 8) ** 2) / 8)
    evening = math.exp(-((hour - 19) ** 2) / 10)
    return 0.1 + morning + evening


def congestion_penalty(hour: int) -> float:
    """Signal penalty during the commute peaks, strongest in the evening."""
    morning = math.exp(-((hour - 8) ** 2) / 6) * 6
    evening = math.exp(-((hour - 19) ** 2) / 8) * 8
    return morning + evening


def generate(total_rows: int, output_path: str) -> list[dict]:
    rng = random.Random(SEED)
    weights = [hourly_volume_weight(h) for h in range(24)]
    per_hour = [max(1, round(total_rows * w / sum(weights))) for w in weights]

    op_w = [o[5] for o in OPERATORS]
    area_w = [a[5] for a in POSTAL_AREAS]
    act_w = [a[3] for a in ACTIVITIES]

    rows = []
    for hour in range(24):
        congestion = congestion_penalty(hour)
        for _ in range(per_hour[hour]):
            network, operator, base_sig, spread, outage_rate, _ = rng.choices(OPERATORS, op_w)[0]
            postal, _town, base_lat, base_lon, urban_bonus, _ = rng.choices(POSTAL_AREAS, area_w)[0]
            activity, speed_range, gps_penalty, _ = rng.choices(ACTIVITIES, act_w)[0]

            lat = round(base_lat + rng.uniform(-0.05, 0.05), 5)
            lon = round(base_lon + rng.uniform(-0.05, 0.05), 5)
            speed = round(rng.uniform(*speed_range), 1)

            signal = base_sig + urban_bonus - congestion + rng.gauss(0, spread)
            signal = int(max(0, min(65, round(signal))))

            outage = outage_rate + (0.15 if signal < 5 else 0)
            status, description = rng.choice(OUTAGE_STATES) if rng.random() < outage else IN_SERVICE

            provider = rng.choices(PROVIDERS, weights=[0.7, 0.3])[0]
            base_precision = 8 if provider == "fused" else 18
            precision = round(base_precision + gps_penalty + rng.uniform(0, 15), 1)
            base_sats = 9 if provider == "fused" else 6
            satellites = float(max(0, round(base_sats - gps_penalty / 6 + rng.gauss(0, 1.5))))

            hhmmss = f"{hour:02d}:{rng.randint(0, 59):02d}:{rng.randint(0, 59):02d}"
            rows.append({
                "hour": hhmmss, "lat": lat, "long": lon, "signal": signal,
                "network": network, "operator": operator, "status": status,
                "description": description, "speed": speed, "satellites": satellites,
                "precission": precision, "provider": provider, "activity": activity,
                "postal_code": postal,
            })

    rows.sort(key=lambda r: r["hour"])
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    return rows


def report(rows: list[dict], output_path: str) -> None:
    print(f"Generated {len(rows):,} rows → {output_path}")
    print(f"Hours covered: {len({r['hour'][:2] for r in rows})} of 24; "
          f"last event {rows[-1]['hour']} ({rows[-1]['postal_code']}, {rows[-1]['operator']})")
    print("\nOperators (avg signal, share):")
    by_op = {}
    for r in rows:
        by_op.setdefault(r["operator"], []).append(r["signal"])
    for op, vals in sorted(by_op.items(), key=lambda kv: -len(kv[1])):
        print(f"  {op:<10} {sum(vals)/len(vals):5.1f}   {100*len(vals)/len(rows):5.1f}%")
    print("\nStatus:")
    for desc, n in Counter(r["description"] for r in rows).most_common():
        print(f"  {desc:<22} {n:>6} ({100*n/len(rows):.1f}%)")
    codes = sorted({r["postal_code"] for r in rows})
    print(f"\nPostal codes ({len(codes)}), for the Athena projection enum:")
    print("  " + ",".join(codes))
    cells = len({(r["hour"][:2], r["postal_code"], r["operator"]) for r in rows})
    print(f"\nHour × postal code × operator cells with data: {cells:,} "
          f"(avg {len(rows)/cells:.1f} readings each)")


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    p.add_argument("--output", default=DEFAULT_OUTPUT)
    a = p.parse_args()
    report(generate(a.rows, a.output), a.output)

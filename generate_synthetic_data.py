"""Generate a realistic but 100% SYNTHETIC automotive business dataset (fictional dealers and models).

Writes to --output (default data/sample):
  dealerships_001.csv / dealerships_002.csv   6 dealerships; file 002 changes one sales target (SCD1 update)
  products_001.csv                            12 fictional vehicle variants
  sales_batch_001.csv                         Jan-Apr 2025 sales (+ defects, duplicates, corrections' originals)
  sales_batch_002.csv                         May-Jun 2025 sales + 12 LATE April sales + 6 corrections
                                              + 4 cross-batch duplicates (+ defects)
  inventory_batch_001.csv / _002.csv          weekly (Sunday) stock snapshots that reconcile exactly with sales
  expected/expected_counts.json               expectations computed here, independent of the pipeline

Stock simulation guarantees:  on_hand(week) = on_hand(prev week) + received(week) - sold(week)
so the inventory reconciliation KPI is exactly zero once ALL sales have arrived; while the late April sales are
missing (after batch 1) the reconciliation flags exactly the affected dealer/product/week cells.
Amounts are in INR. Everything is deterministic for a given --seed.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from src.validation.entities import RAW_COLUMNS

DEALERS = [
    ("D001", "Sunrise Motors", "Pune", "Maharashtra", "West", "2012-03-15", 48_000_000, 1.30),
    ("D002", "Capital Auto Hub", "New Delhi", "Delhi", "North", "2009-07-01", 45_000_000, 1.10),
    ("D003", "Coastline Cars", "Chennai", "Tamil Nadu", "South", "2015-01-20", 42_000_000, 1.00),
    ("D004", "Eastern Wheels", "Kolkata", "West Bengal", "East", "2016-09-10", 36_000_000, 0.80),
    ("D005", "Lakeside Autos", "Bhopal", "Madhya Pradesh", "Central", "2018-11-05", 38_000_000, 0.90),
    ("D006", "Garden City Motors", "Bengaluru", "Karnataka", "South", "2011-05-25", 50_000_000, 1.20),
]
# id, model, variant, segment, fuel, list_price, unit_cost, popularity
PRODUCTS = [
    ("P001", "Zephyr", "LX", "Hatchback", "Petrol", 550_000, 478_000, 1.6),
    ("P002", "Zephyr", "VX", "Hatchback", "Petrol", 640_000, 560_000, 1.2),
    ("P003", "Orion", "LX", "Sedan", "Petrol", 980_000, 855_000, 0.8),
    ("P004", "Orion", "ZX", "Sedan", "Diesel", 1_180_000, 1_030_000, 0.7),
    ("P005", "Titan", "LX", "SUV", "Diesel", 1_450_000, 1_260_000, 1.0),
    ("P006", "Titan", "ZX", "SUV", "Diesel", 1_890_000, 1_640_000, 0.6),
    ("P007", "Titan", "Hybrid", "SUV", "Hybrid", 2_250_000, 1_960_000, 0.3),
    ("P008", "Vega", "LX", "MPV", "Petrol", 1_050_000, 915_000, 0.7),
    ("P009", "Vega", "ZX", "MPV", "CNG", 1_190_000, 1_040_000, 0.6),
    ("P010", "Pulse", "E1", "Hatchback", "EV", 1_150_000, 1_010_000, 0.5),
    ("P011", "Pulse", "E2", "SUV", "EV", 1_790_000, 1_560_000, 0.4),
    ("P012", "Nova", "LX", "Hatchback", "CNG", 610_000, 535_000, 1.3),
]
START, END = date(2025, 1, 1), date(2025, 6, 30)
BATCH1_LAST_SALE = date(2025, 4, 30)
BATCH1_LAST_SNAPSHOT = date(2025, 4, 27)
LATE_WINDOW = (date(2025, 4, 14), date(2025, 4, 27))
PAYMENTS = ["Cash", "Loan", "Loan", "Loan", "Lease"]
CUSTOMERS = ["Retail", "Retail", "Retail", "Corporate", "Fleet"]
DISCOUNTS = [0.0, 0.0, 2.0, 3.0, 5.0, 5.0, 7.5, 10.0]


def _simulate(rng: random.Random) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Day-by-day stock and sales simulation. Returns (sales, weekly_snapshots)."""
    pairs = [(d, p) for d in DEALERS for p in PRODUCTS]
    stock = {(d[0], p[0]): 10 for d, p in pairs}
    received_in_week = {k: 0 for k in stock}
    sales: list[dict[str, Any]] = []
    snaps: list[dict[str, Any]] = []
    day = START
    while day <= END:
        if day.weekday() == 0:  # Monday: replenish
            for key in stock:
                if stock[key] < 6:
                    qty = 14 - stock[key]
                    stock[key] += qty
                    received_in_week[key] += qty
        season = 1.15 if day.month in (3, 5) else 1.0
        weekend = 1.4 if day.weekday() >= 5 else 1.0
        for d, p in pairs:
            key = (d[0], p[0])
            if stock[key] > 0 and rng.random() < 0.10 * p[7] * d[7] * weekend * season:
                qty = min(stock[key], 2 if rng.random() < 0.1 else 1)
                stock[key] -= qty
                sales.append({
                    "date": day, "dealership_id": d[0], "product_id": p[0], "quantity": qty,
                    "unit_price": p[5], "discount_pct": rng.choice(DISCOUNTS),
                    "payment_type": rng.choice(PAYMENTS), "customer_type": rng.choice(CUSTOMERS),
                    "unit_cost": p[6],
                })
        if day.weekday() == 6:  # Sunday: snapshot
            for key in stock:
                snaps.append({"date": day, "dealership_id": key[0], "product_id": key[1],
                              "units_on_hand": stock[key], "units_received": received_in_week[key]})
                received_in_week[key] = 0
        day += timedelta(days=1)
    for i, s in enumerate(sorted(sales, key=lambda r: (r["date"], r["dealership_id"], r["product_id"])), start=1):
        s["sale_id"] = f"S{i:07d}"
    return sales, snaps


def _sale_row(s: dict[str, Any]) -> dict[str, Any]:
    return {
        "sale_id": s["sale_id"], "sale_date": s["date"].isoformat(), "dealership_id": s["dealership_id"],
        "product_id": s["product_id"], "quantity": s["quantity"], "unit_price": s["unit_price"],
        "discount_pct": s["discount_pct"], "payment_type": s["payment_type"], "customer_type": s["customer_type"],
    }


def _snap_row(s: dict[str, Any]) -> dict[str, Any]:
    return {"snapshot_date": s["date"].isoformat(), "dealership_id": s["dealership_id"],
            "product_id": s["product_id"], "units_on_hand": s["units_on_hand"], "units_received": s["units_received"]}


BAD_KINDS = {
    "null_date": ("sale_date", ""),
    "neg_qty": ("quantity", -2),
    "unknown_dealer": ("dealership_id", "D999"),
    "unknown_product": ("product_id", "P999"),
    "discount_150": ("discount_pct", 150),
    "bad_price": ("unit_price", "abc"),
    "bad_date": ("sale_date", "2025-02-31"),
    "missing_id": ("sale_id", ""),
}
BAD_B1 = {"null_date": 3, "neg_qty": 2, "unknown_dealer": 2, "discount_150": 2, "bad_price": 1, "bad_date": 1, "missing_id": 1}
BAD_B2 = {"null_date": 2, "neg_qty": 2, "unknown_dealer": 1, "unknown_product": 3, "discount_150": 1, "bad_price": 1, "bad_date": 1, "missing_id": 1}


def _bad_rows(rng: random.Random, templates: list[dict[str, Any]], plan: dict[str, int], first_id: int) -> list[dict[str, Any]]:
    out, n = [], first_id
    for kind, count in plan.items():
        col, value = BAD_KINDS[kind]
        for _ in range(count):
            row = dict(_sale_row(rng.choice(templates)))
            row["sale_id"] = f"S{n:07d}"
            n += 1
            row[col] = value
            out.append(row)
    return out


def _write_csv(path: Path, columns: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def _money(s: dict[str, Any], discount: float) -> tuple[float, float, float]:
    gross = s["quantity"] * s["unit_price"]
    net = round(gross - gross * discount / 100.0, 2)
    cost = s["quantity"] * s["unit_cost"]
    return net, round(net - cost, 2), float(s["quantity"])


def generate(output_dir: str | Path = "data/sample", seed: int = 42) -> dict[str, Any]:
    """Create sample files; return the expected-counts dictionary (also written to expected/)."""
    rng = random.Random(seed)
    out = Path(output_dir)
    (out / "expected").mkdir(parents=True, exist_ok=True)
    sales, snaps = _simulate(rng)

    # ---- reference data -------------------------------------------------------------------
    dealer_rows = [dict(zip(RAW_COLUMNS["dealerships"], d[:7], strict=True)) for d in DEALERS]
    _write_csv(out / "dealerships_001.csv", RAW_COLUMNS["dealerships"], dealer_rows)
    dealer_rows_2 = [dict(r) for r in dealer_rows]
    dealer_rows_2[1]["monthly_sales_target"] = int(dealer_rows_2[1]["monthly_sales_target"] * 1.10)  # D002 target +10%
    _write_csv(out / "dealerships_002.csv", RAW_COLUMNS["dealerships"], dealer_rows_2)
    _write_csv(out / "products_001.csv", RAW_COLUMNS["products"], [dict(zip(RAW_COLUMNS["products"], p[:7], strict=True)) for p in PRODUCTS])

    # ---- split sales into batches ------------------------------------------------------------
    in_b1 = [s for s in sales if s["date"] <= BATCH1_LAST_SALE]
    late_pool = [s for s in in_b1 if LATE_WINDOW[0] <= s["date"] <= LATE_WINDOW[1]]
    late = rng.sample(late_pool, 12)
    late_ids = {s["sale_id"] for s in late}
    b1 = [s for s in in_b1 if s["sale_id"] not in late_ids]
    b2_new = [s for s in sales if s["date"] > BATCH1_LAST_SALE]

    pool = b1[:]
    rng.shuffle(pool)

    def take(n: int) -> list[dict[str, Any]]:
        picked = pool[:n]
        del pool[:n]
        return picked

    corrections, cross_dups, in_dups = take(6), take(4), take(4)
    blank_discount, blank_customer = take(3), take(2)
    corrected_discount = {s["sale_id"]: min(30.0, s["discount_pct"] + 2.0) for s in corrections}
    blank_disc_ids = {s["sale_id"] for s in blank_discount}

    rows1 = []
    for s in b1:
        row = _sale_row(s)
        if s["sale_id"] in blank_disc_ids:
            row["discount_pct"] = ""
        if s in blank_customer:
            row["customer_type"] = ""
        rows1.append(row)
    rows1 += [dict(_sale_row(s)) for s in in_dups]  # exact in-batch duplicates
    bad1 = _bad_rows(rng, b1, BAD_B1, 9_000_001)
    file1 = rows1 + bad1

    rows2 = [_sale_row(s) for s in b2_new] + [_sale_row(s) for s in late]
    for s in corrections:  # same sale_id, corrected discount
        row = _sale_row(s)
        row["discount_pct"] = corrected_discount[s["sale_id"]]
        rows2.append(row)
    rows2 += [_sale_row(s) for s in cross_dups]  # identical to batch 1 -> must not double count
    bad2 = _bad_rows(rng, b2_new, BAD_B2, 9_100_001)
    file2 = rows2 + bad2
    _write_csv(out / "sales_batch_001.csv", RAW_COLUMNS["sales"], file1)
    _write_csv(out / "sales_batch_002.csv", RAW_COLUMNS["sales"], file2)

    # ---- inventory --------------------------------------------------------------------------
    snaps1 = [s for s in snaps if s["date"] <= BATCH1_LAST_SNAPSHOT]
    snaps2 = [s for s in snaps if s["date"] > BATCH1_LAST_SNAPSHOT]
    inv1 = [_snap_row(s) for s in snaps1]
    inv1 += [dict(_snap_row(s)) for s in rng.sample(snaps1, 3)]  # exact duplicates
    for s in rng.sample(snaps1, 2):  # negative stock
        r = _snap_row(s)
        r["units_on_hand"] = -3
        inv1.append(r)
    orphan = _snap_row(rng.choice(snaps1))
    orphan["dealership_id"] = "D999"
    inv1.append(orphan)
    inv2 = [_snap_row(s) for s in snaps2]
    neg = _snap_row(rng.choice(snaps2))
    neg["units_on_hand"] = -1
    inv2.append(neg)
    _write_csv(out / "inventory_batch_001.csv", RAW_COLUMNS["inventory"], inv1)
    _write_csv(out / "inventory_batch_002.csv", RAW_COLUMNS["inventory"], inv2)

    # ---- expectations (independent of the pipeline) ----------------------------------------
    watermark: dict[str, date] = {}
    for s in b1:
        watermark[s["dealership_id"]] = max(watermark.get(s["dealership_id"], date.min), s["date"])
    late_count = sum(1 for s in late if s["date"] < watermark[s["dealership_id"]])

    def week_end(d: date) -> date:
        return d + timedelta(days=6 - d.weekday())

    recon_cells = {(s["dealership_id"], s["product_id"], week_end(s["date"])) for s in late}
    net = profit = units = 0.0
    months, dealer_months, products = set(), set(), set()
    for s in sales:
        disc = 0.0 if s["sale_id"] in blank_disc_ids else corrected_discount.get(s["sale_id"], s["discount_pct"])
        n, p, u = _money(s, disc)
        net, profit, units = net + n, profit + p, units + u
        months.add(s["date"].strftime("%Y-%m"))
        dealer_months.add((s["date"].strftime("%Y-%m"), s["dealership_id"]))
        products.add(s["product_id"])
    n_snap_total = len(snaps)

    expected = {
        "note": "Computed by the generator from the injected defects and the simulation, independent of the pipeline.",
        "seed": seed,
        "after_batch_1": {
            "sales": {"bronze_rows": len(file1), "rejected_rows": len(bad1), "valid_rows": len(file1) - len(bad1),
                      "in_batch_duplicate_rows_removed": len(in_dups), "rows_with_warnings": len(blank_discount) + len(blank_customer),
                      "late_arrival_rows": 0, "silver_rows": len(b1)},
            "inventory": {"bronze_rows": len(inv1), "rejected_rows": 3, "silver_rows": len(snaps1), "in_batch_duplicate_rows_removed": 3},
            "dealerships": {"silver_rows": 6}, "products": {"silver_rows": 12},
            "reconciliation_exceptions": len(recon_cells),
        },
        "after_batch_2": {
            "sales": {"bronze_rows": len(file2), "rejected_rows": len(bad2), "rows_matching_existing_silver": len(corrections) + len(cross_dups),
                      "rows_changing_existing_silver": len(corrections), "late_arrival_rows": late_count,
                      "silver_rows": len(sales)},
            "inventory": {"bronze_rows": len(inv2), "rejected_rows": 1, "silver_rows": n_snap_total},
            "dealerships": {"silver_rows": 6, "rows_changing_existing_silver": 1},
            "reconciliation_exceptions": 0,
            "gold": {
                "fact_sales_rows": len(sales), "fact_inventory_rows": n_snap_total,
                "total_net_revenue": round(net, 2), "total_gross_profit": round(profit, 2), "total_units": int(units),
                "mart_monthly_sales_rows": len(months), "mart_dealership_monthly_rows": len(dealer_months),
                "mart_product_kpis_rows": len(products), "mart_inventory_weekly_rows": n_snap_total,
            },
        },
        "corrected_sale_ids": sorted(corrected_discount),
        "late_sale_ids": sorted(late_ids),
    }
    (out / "expected" / "expected_counts.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate synthetic automotive MIS sample data")
    parser.add_argument("--output", default="data/sample")
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    print(json.dumps(generate(args.output, args.seed), indent=2))


if __name__ == "__main__":
    main()

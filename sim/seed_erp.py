"""
Reads the M5 files and fills Azure SQL with the company’s opening state: 
10 stores, 3,049 products with price and supplier, and 30,490 inventory rows with opening stock and a reorder point. 
The numbers come from each item’s average sales over the 90 days before 2015-09-24.

Run once:  python sim/seed_erp.py
"""

from __future__ import annotations

import argparse
import math
import zlib
from datetime import timedelta

import pandas as pd

import m5
from common import CFG, STORE_CITY, SUPPLIERS, day_num

DAYS_OF_STOCK = 14     # opening stock and order-up-to level, in days of average demand
SAFETY_DAYS = 2        # reorder point = (supplier lead time + safety days) of demand
MIN_DAILY_DEMAND = 0.1  # floor so slow or not-yet-launched items still get reordered


def supplier_for(item_id: str) -> str:
    """Stable assignment of each product to one of the 5 suppliers."""
    return sorted(SUPPLIERS)[zlib.crc32(item_id.encode()) % len(SUPPLIERS)]


def planning_params(sales: pd.DataFrame, start_day: str) -> pd.DataFrame:
    """Average daily demand over the 90 days before start_day, plus order levels."""
    s = day_num(start_day)
    hist = [c for c in sales.columns if c.startswith("d_") and s - m5.HISTORY_DAYS <= day_num(c) < s]
    p = pd.DataFrame(index=sales.index)
    p["avg_daily"] = sales[hist].mean(axis=1).clip(lower=MIN_DAILY_DEMAND)
    p["supplier_id"] = [supplier_for(i) for i in p.index.get_level_values("item_id")]
    lead = p["supplier_id"].map(lambda x: SUPPLIERS[x]["lead_days"])
    p["reorder_point"] = [math.ceil(a * (l + SAFETY_DAYS)) for a, l in zip(p["avg_daily"], lead)]
    p["order_up_to"] = [math.ceil(a * DAYS_OF_STOCK) for a in p["avg_daily"]]
    return p


def build_seed(start_day: str):
    cal = m5.calendar()
    sales = m5.sales(start_day)
    prices = m5.weekly_item_prices()
    params = planning_params(sales, start_day)

    start_date = cal.loc[start_day, "date"]
    seed_ts = (start_date - timedelta(days=1)).to_pydatetime()
    week = cal.loc[start_day, "wm_yr_wk"]

    meta = sales.reset_index()[["store_id", "state_id"]].drop_duplicates()
    stores = [(r.store_id, r.state_id, STORE_CITY[r.state_id]) for r in meta.itertuples()]

    items = sales.reset_index()[["item_id", "dept_id", "cat_id"]].drop_duplicates("item_id")
    products = [(r.item_id, r.dept_id, r.cat_id, float(prices.loc[r.item_id, week]),
                 supplier_for(r.item_id), seed_ts) for r in items.itertuples()]

    inventory = [(s, i, int(row.order_up_to), int(row.reorder_point), seed_ts)
                 for (s, i), row in params.iterrows()]
    return stores, products, inventory


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-day", default=CFG.start_day)
    args = ap.parse_args()

    from erp import SqlErp
    stores, products, inventory = build_seed(args.start_day)
    print(f"Seeding {len(stores)} stores, {len(products)} products, {len(inventory)} inventory rows "
          f"(opening stock = {DAYS_OF_STOCK} days of demand before {args.start_day})")
    SqlErp().seed(stores, products, inventory)
    print("Done.")


if __name__ == "__main__":
    main()

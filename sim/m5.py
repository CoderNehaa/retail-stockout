"""Loads the parts of the M5 dataset the simulator needs."""
from __future__ import annotations

import pandas as pd

from common import CFG, day_num

ID_COLS = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
HISTORY_DAYS = 90  # used for opening stock and reorder points


def calendar() -> pd.DataFrame:
    cal = pd.read_csv(CFG.m5_dir / "calendar.csv", parse_dates=["date"])
    return cal.set_index("d")


def sales(first_day: str, last_day: str = "d_1941") -> pd.DataFrame:
    """Wide daily unit sales (true demand) per store-item, from first_day - 90 days."""
    lo = max(1, day_num(first_day) - HISTORY_DAYS)
    hi = day_num(last_day)
    path = CFG.m5_dir / "sales_train_evaluation.csv"
    header = pd.read_csv(path, nrows=0).columns
    days = [c for c in header if c.startswith("d_") and lo <= day_num(c) <= hi]
    df = pd.read_csv(path, usecols=ID_COLS + days, dtype={d: "int32" for d in days})
    return df.set_index(["store_id", "item_id"]).sort_index()


def weekly_item_prices() -> pd.DataFrame:
    """One price per item and Walmart week: the median across the 10 stores.

    dbo.products has one unit_price per item, so per-store prices are collapsed.
    Returns a DataFrame indexed by item_id with one column per wm_yr_wk.
    """
    cache = CFG.state_dir / "weekly_item_prices.pkl"
    if cache.exists():
        return pd.read_pickle(cache)
    sp = pd.read_csv(CFG.m5_dir / "sell_prices.csv")
    wide = (sp.groupby(["item_id", "wm_yr_wk"])["sell_price"].median().round(2)
              .unstack("wm_yr_wk").sort_index(axis=1))
    # Before an item launches it has no price: back-fill with its first known price,
    # and carry the last price forward through gaps.
    wide = wide.ffill(axis=1).bfill(axis=1)
    CFG.state_dir.mkdir(parents=True, exist_ok=True)
    wide.to_pickle(cache)
    return wide

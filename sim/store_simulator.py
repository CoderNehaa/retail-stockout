"""Plays back M5 one simulated day at a time as a live retail company.

For each simulated day it:
  1. applies that week's prices to dbo.products (CDC row per change),
  2. receives supplier deliveries that arrive today,
  3. sells min(M5 demand, on-hand stock) and sends each sale to Event Hubs,
  4. reorders items at or below their reorder point; suppliers ship the same day,
  5. writes supplier files (shipped + delivered lines, with deliberate mess) and weather,
  6. writes the changed stock levels to dbo.inventory (CDC row per change),
  7. appends true demand vs. actual sales to sim_truth.csv.

Every output is deterministic per (seed, day): rerunning a day after a crash sends the
same event_ids and overwrites the same files, so the platform's dedup handles it.

Examples:
  python sim/store_simulator.py --days 90 --minutes-per-day 0     # fast backfill
  python sim/store_simulator.py --days 6                          # live, 10 min per day
  python sim/store_simulator.py --dry-run --days 5                # local only, no Azure
"""
from __future__ import annotations

import argparse
import json
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

import m5
from common import CFG, ROOT, SUPPLIERS, Landing, day_num, secret
from seed_erp import build_seed, planning_params
from weather_pull import weather_rows

DUPLICATE_ROW_RATE = 0.02   # supplier rows sent twice
LATE_FILE_RATE = 0.05       # supplier files that land a day late
POS_RETRY_RATE = 0.01       # POS events resent with the same event_id
SCHEMA_DRIFT_DAY = 60       # from this simulated day of the run, SUP_3 renames qty -> quantity
CARRIERS = ["Estes", "XPO Logistics", "Old Dominion", "Saia"]
# 1 throughput unit accepts 1,000 events/s; stay below it so sends aren't throttled
EH_MAX_EVENTS_PER_SEC = float(os.getenv("EH_MAX_EVENTS_PER_SEC", "800"))
EH_BATCH_EVENTS = 400
EVENT_NS = uuid.UUID("6f1c2d3e-4b5a-4c7d-8e9f-a0b1c2d3e4f5")


# ============================================================================ state

class State:
    """What the ERP tables don't hold: shipments in transit, held late files, the clock."""

    def __init__(self, path: Path, start_day: str):
        self.path = path
        if path.exists():
            d = json.loads(path.read_text())
        else:
            d = {"next_day": start_day, "run_day": 1, "in_transit": [], "held_files": [],
                 "last_week": None}
        self.__dict__.update(d)

    def save(self):
        d = {k: v for k, v in self.__dict__.items() if k != "path"}
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(d, default=str))
        tmp.replace(self.path)

    def on_order(self) -> dict:
        out = defaultdict(int)
        for s in self.in_transit:
            out[(s["store_id"], s["item_id"])] += s["qty"]
        return out


# ============================================================================ outputs

class PosSender:
    def __init__(self, dry_run: bool, events_dir: Path):
        self.dry_run = dry_run
        self.events_dir = events_dir
        self._last_send = 0.0
        if not dry_run:
            from azure.eventhub import EventHubProducerClient
            self.client = EventHubProducerClient.from_connection_string(
                secret("eh-pos-send"), eventhub_name=CFG.eh_name)

    def send(self, sim_day: str, events: list[dict]) -> None:
        if self.dry_run:
            self.events_dir.mkdir(parents=True, exist_ok=True)
            with open(self.events_dir / f"{sim_day}.jsonl", "a", encoding="utf-8") as f:
                for e in events:
                    f.write(json.dumps(e) + "\n")
            return
        from azure.eventhub import EventData
        by_store = defaultdict(list)
        for e in events:
            by_store[e["store_id"]].append(e)
        for store, evs in by_store.items():  # partition key = store_id keeps a store's events in order
            for i in range(0, len(evs), EH_BATCH_EVENTS):
                chunk = evs[i:i + EH_BATCH_EVENTS]
                batch = self.client.create_batch(partition_key=store)
                for e in chunk:
                    ed = EventData(json.dumps(e))
                    try:
                        batch.add(ed)
                    except ValueError:  # batch full by size
                        self._send(batch, len(batch))
                        batch = self.client.create_batch(partition_key=store)
                        batch.add(ed)
                if len(batch):
                    self._send(batch, len(batch))

    def _send(self, batch, n: int) -> None:
        """Send one batch, staying under the namespace's events/second limit (1 TU = 1,000/s)."""
        from azure.eventhub.exceptions import EventHubError
        wait = n / EH_MAX_EVENTS_PER_SEC - (time.monotonic() - self._last_send)
        if wait > 0:
            time.sleep(wait)
        for attempt in range(1, 11):
            try:
                self.client.send_batch(batch)
                break
            except EventHubError as e:
                if "throttled" not in str(e).lower() and "server-busy" not in str(e).lower():
                    raise
                if attempt == 10:
                    raise
                print(f"  Event Hubs throttled, waiting 5s ({attempt}/10)", flush=True)
                time.sleep(5)
        self._last_send = time.monotonic()

    def close(self):
        if not self.dry_run:
            self.client.close()


def supplier_file_rows(rows: list[dict], supplier: str, run_day: int, rng: random.Random) -> list[dict]:
    out = []
    for r in rows:
        r = dict(r)
        if supplier == "SUP_3" and run_day >= SCHEMA_DRIFT_DAY:  # schema drift
            r["quantity"] = r.pop("qty")
            r["carrier"] = rng.choice(CARRIERS)
        out.append(r)
        if rng.random() < DUPLICATE_ROW_RATE:  # duplicate line
            out.append(dict(r))
    return out


# ============================================================================ simulator

class Simulator:
    def __init__(self, dry_run: bool, minutes_per_day: float, start_day: str):
        self.dry_run = dry_run
        self.minutes = minutes_per_day
        if dry_run:  # a dry run always starts fresh, because its in-memory ERP isn't saved
            import shutil
            shutil.rmtree(ROOT / "out", ignore_errors=True)
        state_dir = ROOT / "out" / "sim_state" if dry_run else CFG.state_dir
        state_dir.mkdir(parents=True, exist_ok=True)
        self.truth_path = (ROOT / "out" if dry_run else ROOT) / "sim_truth.csv"

        print("Loading M5 ...")
        self.cal = m5.calendar()
        self.sales = m5.sales(start_day)
        self.prices = m5.weekly_item_prices()
        self.params = planning_params(self.sales, start_day)
        self.state = State(state_dir / "state.json", start_day)

        if dry_run:
            from erp import DryErp
            self.erp = DryErp()
            self.erp.seed(*build_seed(start_day))
        else:
            from erp import SqlErp
            self.erp = SqlErp()
        print("Reading inventory from the ERP ...")
        self.inventory = self.erp.load_inventory()
        self.products = self.erp.load_products()
        self.landing = Landing(dry_run)
        self.pos = PosSender(dry_run, ROOT / "out" / "events")
        self._trim_truth()

    def _trim_truth(self):
        """After a crash, drop truth rows for days that will be replayed."""
        if self.truth_path.exists():
            t = pd.read_csv(self.truth_path)
            keep = t[t["sim_day"].map(day_num) < day_num(self.state.next_day)]
            if len(keep) < len(t):
                keep.to_csv(self.truth_path, index=False)

    # ------------------------------------------------------------------ one day
    def run_day(self) -> dict:
        st = self.state
        d = st.next_day
        if d not in self.sales.columns:
            raise StopIteration(f"M5 has no sales for {d}; the playback is finished.")
        rng = random.Random(f"{CFG.seed}-{d}")
        date = self.cal.loc[d, "date"].date()
        day_start = datetime.combine(date, datetime.min.time())
        eod_ts = day_start + timedelta(hours=22)
        stats = defaultdict(int)

        # 0. late files from yesterday land now, under their original date
        for f in st.held_files:
            self.landing.write_jsonl(f["path"], f["rows"])
            stats["late_files_landed"] += 1
        st.held_files = []

        # 1. weekly prices
        week = int(self.cal.loc[d, "wm_yr_wk"])
        price_rows = []
        if week != st.last_week:
            for item, p in self.products.items():
                new = float(self.prices.loc[item, week]) if week in self.prices.columns else p["unit_price"]
                if round(new, 2) != round(p["unit_price"], 2):
                    price_rows.append((item, round(new, 2), day_start))
                    p["unit_price"] = round(new, 2)
            st.last_week = week
        stats["price_changes"] = len(price_rows)

        changed = set()
        file_rows = defaultdict(list)  # supplier -> rows for today's file

        # 2. deliveries arriving today
        still = []
        for s in st.in_transit:
            if s["arrival_date"] <= date.isoformat():
                key = (s["store_id"], s["item_id"])
                self.inventory[key][0] += s["qty"]
                changed.add(key)
                file_rows[s["supplier_id"]].append({
                    "shipment_id": s["shipment_id"], "po_id": s["po_id"], "store_id": s["store_id"],
                    "item_id": s["item_id"], "qty": s["qty"], "ship_date": s["ship_date"],
                    "expected_arrival": s["expected_arrival"], "event_type": "delivered",
                    "delivered_date": s["arrival_date"]})
                stats["lines_delivered"] += 1
            else:
                still.append(s)
        st.in_transit = still

        # 3. sales: a store can't sell more than it has
        demand = self.sales[d]
        events, truth = [], []
        for (store, item), want in demand.items():
            if want <= 0:
                continue
            inv = self.inventory[(store, item)]
            sold = min(int(want), max(inv[0], 0))
            truth.append((store, item, d, date.isoformat(), int(want), sold, int(want) - sold))
            if sold == 0:
                continue
            inv[0] -= sold
            changed.add((store, item))
            price = self.products[item]["unit_price"]
            left, n = sold, 0
            while left > 0:  # split the day's units into individual baskets
                q = min(left, rng.choice([1, 1, 1, 2, 2, 3, 4, 6]))
                left -= q
                ts = day_start + timedelta(seconds=rng.randint(8 * 3600, 22 * 3600 - 1))
                events.append({
                    "event_id": str(uuid.uuid5(EVENT_NS, f"{d}|{store}|{item}|{n}")),
                    "store_id": store, "item_id": item, "qty": q, "unit_price": price,
                    "event_ts": ts.strftime("%Y-%m-%dT%H:%M:%SZ"), "sim_day": d})
                n += 1
        events.sort(key=lambda e: e["event_ts"])
        retries = [dict(e) for e in events if rng.random() < POS_RETRY_RATE]  # POS retries
        stats["pos_events"] = len(events)
        stats["pos_retries"] = len(retries)
        stats["lost_units"] = sum(t[6] for t in truth)
        stats["stockout_items"] = sum(1 for t in truth if t[6] > 0)

        # 4. reorders: suppliers ship today, one shipment per supplier and store
        on_order = st.on_order()
        orders = defaultdict(list)
        for key, (qty, rp) in self.inventory.items():
            pos = qty + on_order.get(key, 0)
            if pos <= rp:
                order_qty = int(self.params.at[key, "order_up_to"]) - pos
                if order_qty > 0:
                    orders[(self.products[key[1]]["supplier_id"], key[0])].append((key[1], order_qty))
        for (sup, store), lines in sorted(orders.items()):
            cfg = SUPPLIERS[sup]
            expected = date + timedelta(days=cfg["lead_days"])
            arrival = expected if rng.random() < cfg["on_time"] else expected + timedelta(days=rng.randint(1, 4))
            shipment_id = f"SHP-{sup}-{store}-{date:%Y%m%d}"
            po_id = f"PO-{store}-{sup}-{date:%Y%m%d}"
            for item, q in lines:
                rec = {"shipment_id": shipment_id, "po_id": po_id, "supplier_id": sup, "store_id": store,
                       "item_id": item, "qty": q, "ship_date": date.isoformat(),
                       "expected_arrival": expected.isoformat(), "arrival_date": arrival.isoformat()}
                st.in_transit.append(rec)
                file_rows[sup].append({k: rec[k] for k in ("shipment_id", "po_id", "store_id", "item_id",
                                                           "qty", "ship_date", "expected_arrival")}
                                      | {"event_type": "shipped"})
                stats["lines_shipped"] += 1

        # 5a. supplier files (5% held back a day) and weather
        for sup in sorted(file_rows):
            rows = supplier_file_rows(file_rows[sup], sup, st.run_day, rng)
            path = f"shipments/{sup}/{date.isoformat()}.json"
            if rng.random() < LATE_FILE_RATE:
                st.held_files.append({"path": path, "rows": rows})
                stats["files_held_late"] += 1
            else:
                self.landing.write_jsonl(path, rows)
            stats["supplier_files"] += 1
        self.landing.write_jsonl(f"weather/{date.isoformat()}.json", weather_rows(date, offline=self.dry_run))

        # 5b. POS events, spread across the simulated day's wall-clock minutes
        self._stream(d, events, retries)

        # 6. ERP writes (last, so a crash before here replays the day identically)
        self.erp.update_prices(price_rows)
        inv_rows = [(s, i, self.inventory[(s, i)][0], eod_ts) for s, i in sorted(changed)]
        self.erp.update_inventory(inv_rows)
        stats["inventory_updates"] = len(inv_rows)

        # 7. ground truth, then advance the clock
        pd.DataFrame(truth, columns=["store_id", "item_id", "sim_day", "date", "demand", "sold", "lost"]).to_csv(
            self.truth_path, mode="a", header=not self.truth_path.exists(), index=False)
        st.next_day = f"d_{day_num(d) + 1}"
        st.run_day += 1
        st.save()
        return {"day": d, "date": date.isoformat(), **stats}

    def _stream(self, d: str, events: list[dict], retries: list[dict]) -> None:
        if self.minutes <= 0:
            self.pos.send(d, events + retries)
            return
        slices = max(1, int(round(self.minutes)))  # about one send per wall-clock minute
        per = max(1, -(-len(events) // slices))
        chunks = [events[i:i + per] for i in range(0, len(events), per)] or [[]]
        retry_chunks = defaultdict(list)
        for r in retries:  # a retry is resent a little later than the original
            retry_chunks[min(len(chunks) - 1, random.Random(r["event_id"]).randint(1, 3))].append(r)
        slot = self.minutes * 60 / len(chunks)
        for i, chunk in enumerate(chunks):
            t0 = time.monotonic()
            self.pos.send(d, chunk + retry_chunks[i])
            time.sleep(max(0.0, slot - (time.monotonic() - t0)))

    def close(self):
        self.pos.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=0, help="simulated days to run (0 = until stopped)")
    ap.add_argument("--minutes-per-day", type=float, default=CFG.minutes_per_day,
                    help="wall-clock minutes per simulated day (0 = as fast as possible)")
    ap.add_argument("--start-day", default=CFG.start_day, help="first M5 day, used only on the first run")
    ap.add_argument("--dry-run", action="store_true", help="no Azure: write everything under out/")
    a = ap.parse_args()

    sim = Simulator(a.dry_run, a.minutes_per_day, a.start_day)
    print(f"Starting at {sim.state.next_day} (run day {sim.state.run_day}), "
          f"{a.minutes_per_day:g} min per simulated day. Ctrl+C to stop; rerun to resume.")
    n = 0
    try:
        while a.days == 0 or n < a.days:
            t0 = time.monotonic()
            s = defaultdict(int, sim.run_day())
            n += 1
            print(f"{s['day']} {s['date']}: {s['pos_events']:>6} events (+{s['pos_retries']} retries) | "
                  f"{s['inventory_updates']:>5} stock changes | {s['lines_shipped']:>4} shipped, "
                  f"{s['lines_delivered']:>4} delivered | {s['stockout_items']:>4} items short, "
                  f"{s['lost_units']:>5} lost units | {s['price_changes']} price changes | "
                  f"{s['files_held_late']} late files | {time.monotonic() - t0:.0f}s", flush=True)
    except StopIteration as e:
        print(e)
    except KeyboardInterrupt:
        print("\nStopped. State is saved after each completed day; rerun to resume.")
    finally:
        sim.close()


if __name__ == "__main__":
    main()
    
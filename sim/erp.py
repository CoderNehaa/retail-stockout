"""Reads and writes the simulated ERP (Azure SQL sqldb-erp).

Every UPDATE on dbo.inventory or dbo.products becomes a CDC row, which Section 3
extracts. updated_at is always the simulated business time, never the wall clock,
because Section 4 sequences SCD2 versions by it.
"""
from __future__ import annotations

from datetime import datetime


class SqlErp:
    def __init__(self):
        from common import sql_connect
        self._connect = sql_connect

    def _run(self, fn):
        conn = self._connect()
        try:
            cur = conn.cursor()
            cur.fast_executemany = True
            out = fn(cur)
            conn.commit()
            return out
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()  # closed connections let the serverless DB auto-pause

    # ---------------------------------------------------------------- seed
    def seed(self, stores, products, inventory) -> None:
        def go(cur):
            cur.execute("SELECT COUNT(*) FROM dbo.inventory")
            if cur.fetchone()[0]:
                raise SystemExit("dbo.inventory already has rows. Seed only once "
                                 "(see 'Starting over' in the guide).")
            cur.executemany("INSERT INTO dbo.stores (store_id, state_id, city) VALUES (?, ?, ?)", stores)
            cur.executemany("INSERT INTO dbo.products (item_id, dept_id, cat_id, unit_price, supplier_id, "
                            "updated_at) VALUES (?, ?, ?, ?, ?, ?)", products)
            cur.executemany("INSERT INTO dbo.inventory (store_id, item_id, on_hand_qty, reorder_point, "
                            "updated_at) VALUES (?, ?, ?, ?, ?)", inventory)
        self._run(go)

    # ---------------------------------------------------------------- reads
    def load_inventory(self) -> dict:
        def go(cur):
            cur.execute("SELECT store_id, item_id, on_hand_qty, reorder_point FROM dbo.inventory")
            return {(s, i): [q, r] for s, i, q, r in cur.fetchall()}
        return self._run(go)

    def load_products(self) -> dict:
        def go(cur):
            cur.execute("SELECT item_id, unit_price, supplier_id FROM dbo.products")
            return {i: {"unit_price": float(p), "supplier_id": s} for i, p, s in cur.fetchall()}
        return self._run(go)

    # ---------------------------------------------------------------- writes
    def update_inventory(self, rows: list[tuple[str, str, int, datetime]]) -> None:
        """rows = (store_id, item_id, new_on_hand_qty, business_ts). One CDC row each."""
        if not rows:
            return

        def go(cur):
            cur.execute("IF OBJECT_ID('tempdb..#inv') IS NOT NULL DROP TABLE #inv;"
                        "CREATE TABLE #inv (store_id VARCHAR(10), item_id VARCHAR(30), qty INT, ts DATETIME2)")
            cur.executemany("INSERT INTO #inv VALUES (?, ?, ?, ?)", rows)
            cur.execute("UPDATE i SET i.on_hand_qty = u.qty, i.updated_at = u.ts "
                        "FROM dbo.inventory i JOIN #inv u ON u.store_id = i.store_id AND u.item_id = i.item_id")
        self._run(go)

    def update_prices(self, rows: list[tuple[str, float, datetime]]) -> None:
        """rows = (item_id, new_unit_price, business_ts). One CDC row (and later SCD2 version) each."""
        if not rows:
            return

        def go(cur):
            cur.execute("IF OBJECT_ID('tempdb..#px') IS NOT NULL DROP TABLE #px;"
                        "CREATE TABLE #px (item_id VARCHAR(30), price DECIMAL(8,2), ts DATETIME2)")
            cur.executemany("INSERT INTO #px VALUES (?, ?, ?)", rows)
            cur.execute("UPDATE p SET p.unit_price = x.price, p.updated_at = x.ts "
                        "FROM dbo.products p JOIN #px x ON x.item_id = p.item_id")
        self._run(go)


class DryErp:
    """In-memory stand-in for --dry-run: same interface, no Azure."""

    def __init__(self):
        self.inventory: dict = {}
        self.products: dict = {}
        self.inv_updates = 0
        self.price_updates = 0

    def seed(self, stores, products, inventory):
        self.products = {p[0]: {"unit_price": float(p[3]), "supplier_id": p[4]} for p in products}
        self.inventory = {(s, i): [q, r] for s, i, q, r, _ in inventory}

    def load_inventory(self):
        return {k: list(v) for k, v in self.inventory.items()}

    def load_products(self):
        return {k: dict(v) for k, v in self.products.items()}

    def update_inventory(self, rows):
        for s, i, q, _ in rows:
            self.inventory[(s, i)][0] = q
        self.inv_updates += len(rows)

    def update_prices(self, rows):
        for i, p, _ in rows:
            self.products[i]["unit_price"] = p
        self.price_updates += len(rows)

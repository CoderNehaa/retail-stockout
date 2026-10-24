"""Upload M5's calendar.csv (holidays, events, SNAP days) to landing container in your storage account.

Run once:  python sim/upload_reference.py
"""
from common import CFG, Landing


def main():
    src = CFG.m5_dir / "calendar.csv"
    Landing(dry_run=False).write_bytes("reference/calendar.csv", src.read_bytes())
    print(f"Uploaded {src.name} to {CFG.landing_container}/reference/calendar.csv")


if __name__ == "__main__":
    main()

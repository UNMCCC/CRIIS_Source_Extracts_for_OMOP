#!/usr/bin/env python3
"""
Probe both MS SQL Server connections (source: unmmgdss, destination: Delphi)
using Kerberos authentication. Reads connection settings from config.yaml.

Requirements:
  pip install pyodbc pyyaml

Linux prerequisites:
  - krb5-user / krb5-workstation package installed
  - Valid Kerberos ticket (run: kinit <username>@DOMAIN.COM)
  - ODBC Driver 17 or 18 for SQL Server installed
  - /etc/krb5.conf configured for your realm

Windows prerequisites:
  - ODBC Driver 17 or 18 for SQL Server installed
  - Logged in as a domain user (ticket obtained automatically)
"""

import pathlib
import subprocess
import sys

import pyodbc

import db

CONFIG_PATH = pathlib.Path(__file__).parent / "config.yaml"


def check_kerberos_ticket() -> bool:
    if sys.platform.startswith("win"):
        print("[INFO] Windows detected — Kerberos ticket managed by OS.")
        return True
    try:
        result = subprocess.run(["klist"], capture_output=True, text=True)
        if result.returncode != 0:
            print("[ERROR] No valid Kerberos ticket found. Run: kinit <user>@REALM")
            print(result.stderr.strip())
            return False
        print("[OK] Kerberos ticket found:")
        for line in result.stdout.splitlines()[:6]:
            print(f"       {line}")
        return True
    except FileNotFoundError:
        print("[WARN] klist not found — skipping ticket check.")
        return True


def probe(label: str, server_cfg: dict) -> bool:
    conn_str = db.make_conn_str(server_cfg)
    print(f"\n[INFO] Connecting to {label}: {server_cfg['server']} / {server_cfg['database']}")
    try:
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION AS version, DB_NAME() AS db;")
        row = cursor.fetchone()
        print(f"[OK] {label} connection succeeded.")
        print(f"       db:      {row.db}")
        print(f"       version: {row.version.splitlines()[0]}")
        cursor.close()
        conn.close()
        return True
    except pyodbc.Error as e:
        print(f"[ERROR] {label} connection failed: {e}")
        return False


def main() -> None:
    print("=" * 60)
    print("  MS SQL Server Kerberos Connection Test (source + destination)")
    print("=" * 60)

    if not check_kerberos_ticket():
        sys.exit(1)

    cfg = db.load_config(CONFIG_PATH)
    src_ok = probe("source", cfg["source"])
    dst_ok = probe("destination", cfg["destination"])

    print("=" * 60)
    sys.exit(0 if (src_ok and dst_ok) else 1)


if __name__ == "__main__":
    main()

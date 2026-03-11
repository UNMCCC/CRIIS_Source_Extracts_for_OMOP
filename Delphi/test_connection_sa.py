#!/usr/bin/env python3
"""
Test MS SQL Server connection using Kerberos authentication.

Requirements:
  pip install pyodbc

Linux prerequisites:
  - krb5-user / krb5-workstation package installed
  - Valid Kerberos ticket (run: kinit <username>@DOMAIN.COM)
  - ODBC Driver 17 or 18 for SQL Server installed
  - /etc/krb5.conf configured for your realm

Windows prerequisites:
  - ODBC Driver 17 or 18 for SQL Server installed
  - Logged in as a domain user (ticket obtained automatically)
"""

import sys
import subprocess
import argparse
import urllib
import pandas as pd
#from sqlalchemy import create_engine


DRIVER = "ODBC Driver 18 for SQL Server"
SERVER = "MGBBRPSQLDBS1\\UNMMGSQLDWPROD"
DATABASE = "unmmgdss"


def check_kerberos_ticket():
    """Verify a valid Kerberos ticket exists (Linux only)."""
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
        for line in result.stdout.splitlines()[:6]:  # show first few lines
            print(f"       {line}")
        return True
    except FileNotFoundError:
        print("[WARN] klist not found — skipping ticket check.")
        return True


def get_connection_string(server=SERVER, database=DATABASE, driver=DRIVER):
    """
    
    """
    params = urllib.parse.quote_plus(
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return f"mssql+pyodbc:///?odbc_connect={params}"

def test_connection(connection_string, query="SELECT @@VERSION AS version;"):
    """Attempt to connect and run a test query."""

    print(f"\n[INFO] Connecting...")
    print(f"       DSN: {connection_string.replace(chr(10), ' ')}")

    try:
        conn = pyodbc.connect(connection_string, timeout=15)
        print("[OK] Connection established.")

        cursor = conn.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        print("\n[OK] Test query succeeded:")
        if row:
            for col, val in zip([d[0] for d in cursor.description], row):
                print(f"       {col}: {val}")

        cursor.close()
        conn.close()
        print("\n[OK] Connection closed cleanly.")
        return True

    except pyodbc.InterfaceError as e:
        print(f"\n[ERROR] Interface error (driver/DSN issue): {e}")
    except pyodbc.OperationalError as e:
        print(f"\n[ERROR] Operational error (server unreachable or auth failed): {e}")
    except pyodbc.Error as e:
        print(f"\n[ERROR] ODBC error: {e}")

    return False


def main():
    print("=" * 60)
    print("  MS SQL Server Kerberos Connection Test")
    print("=" * 60)

    # Step 1: Check Kerberos ticket
    if not check_kerberos_ticket():
        sys.exit(1)

    # Step 2: Build connection string and test
    sql = """
        SELECT top 100 *
        FROM [unmmgdss].[p126_reporting].[encounter]
    """
    conn_str = get_connection_string()
    df = test_connection(conn_str, sql)

    print(f"\n[INFO] Examining Results...")
    print(f"       DF: {df.info()}")

    print("=" * 60)
    sys.exit(1 if df.empty else 0)


if __name__ == "__main__":
    main()

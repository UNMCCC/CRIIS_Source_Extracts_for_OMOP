#!/usr/bin/env python3
"""
Test MS SQL Server connection using Kerberos authentication.

Requirements:
  pip install pyodbc

Linux prerequisites:
  - krb5-user / krb5-workstation package installed
  - Valid Kerberos ticket (run: kinit <username>@DOMAIN.COM)
  - ODBC Driver 18 for SQL Server installed
  - /etc/krb5.conf configured for your realm

Windows prerequisites:
  - ODBC Driver 18 for SQL Server installed
  - Logged in as a domain user (ticket obtained automatically)
"""

import sys
import subprocess

DRIVER = "ODBC Driver 18 for SQL Server"
# SOURCE:
#SERVER = "MGBBRPSQLDBS1\\UNMMGSQLDWPROD"
#DATABASE = "unmmgdss"
# DESTINATION:
SERVER = "unmmg-sql-ccc\\unmmgsqlunmccc"
DATABASE = "Delphi"

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


def list_odbc_drivers():
    """Print available ODBC drivers that match SQL Server."""
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if "SQL Server" in d]
        if not drivers:
            print("[ERROR] No SQL Server ODBC drivers found.")
            print("        Install: https://learn.microsoft.com/sql/connect/odbc/download-odbc-driver-for-sql-server")
            return None
        print("[OK] Available SQL Server ODBC drivers:")
        for d in drivers:
            print(f"       - {d}")
        # Prefer the latest numbered driver (e.g. "ODBC Driver 18") over the legacy "SQL Server" driver
        numbered = [d for d in drivers if d != "SQL Server"]
        preferred = sorted(numbered, reverse=True)[0] if numbered else drivers[0]
        print(f"[INFO] Using driver: {preferred}")
        return preferred
    except ImportError:
        print("[ERROR] pyodbc is not installed. Run: pip install pyodbc")
        sys.exit(1)


def build_connection_string(server, database, driver):
    """
    Build a Kerberos (Windows Integrated / Trusted) connection string.

    On Linux, 'Trusted_Connection=yes' triggers GSSAPI/Kerberos when the
    ODBC driver detects a Kerberos ticket via kinit.
    """
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Trusted_Connection=yes;"        # Kerberos / Windows Auth
        f"Encrypt=yes;"                   # Enable TLS encryption
        f"TrustServerCertificate=yes;"    # Set to 'no' in production with valid cert
    )


def test_connection(connection_string, query="SELECT @@VERSION AS version;"):
    """Attempt to connect and run a test query."""
    import pyodbc

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

    ## Step 2: Resolve ODBC driver
    #driver = list_odbc_drivers()
    #if not driver:
    #    sys.exit(1)

    # Step 3: Build connection string and test
    #conn_str = build_connection_string(args.server, args.database, driver, args.port)
    conn_str = build_connection_string(SERVER, DATABASE, DRIVER)
    success = test_connection(conn_str)

    print("=" * 60)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

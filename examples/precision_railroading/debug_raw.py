import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Schema of raw_clm_events:")
cursor.execute("PRAGMA table_info(raw_clm_events)")
for row in cursor.fetchall():
    print(f"  {row}")

print("\nFirst 5 rows from raw_clm_events:")
cursor.execute("SELECT * FROM raw_clm_events LIMIT 5")
for row in cursor.fetchall():
    print(f"  {row}")

conn.close()

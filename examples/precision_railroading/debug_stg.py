import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Schema of stg_clm_events:")
cursor.execute("PRAGMA table_info(stg_clm_events)")
for row in cursor.fetchall():
    print(f"  {row}")

print("\nFirst 3 rows from stg_clm_events:")
cursor.execute("SELECT event_id, car_number, timestamp, event_type FROM stg_clm_events LIMIT 3")
for row in cursor.fetchall():
    print(f"  {row}")

print("\nTest casting:")
cursor.execute("SELECT timestamp, CAST(timestamp AS TIMESTAMP) as ts_cast, CAST(timestamp AS TEXT) as ts_text FROM raw_clm_events LIMIT 3")
for row in cursor.fetchall():
    print(f"  Raw: {row[0]}, CAST TIMESTAMP: {row[1]}, CAST TEXT: {row[2]}")

conn.close()

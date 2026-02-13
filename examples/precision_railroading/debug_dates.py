import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Sample timestamps from stg_clm_events:")
cursor.execute("SELECT timestamp FROM stg_clm_events LIMIT 5")
for row in cursor.fetchall():
    print(f"  {row[0]}")

print("\nSample dates from dim_date:")
cursor.execute("SELECT date FROM dim_date WHERE date >= '2024-01-01' AND date <= '2024-01-05' ORDER BY date")
for row in cursor.fetchall():
    print(f"  {row[0]}")

print("\nDate range in dim_date:")
cursor.execute("SELECT MIN(date), MAX(date) FROM dim_date")
row = cursor.fetchone()
print(f"  Min: {row[0]}, Max: {row[1]}")

print("\nChecking timestamp casting:")
cursor.execute("SELECT timestamp, CAST(timestamp AS DATE) as date_part FROM stg_clm_events LIMIT 3")
for row in cursor.fetchall():
    print(f"  Timestamp: {row[0]}, Date part: {row[1]}")

print("\nJoin test:")
cursor.execute("""
SELECT e.timestamp, CAST(e.timestamp AS DATE) as event_date, d.date 
FROM stg_clm_events e
LEFT JOIN dim_date d ON CAST(e.timestamp AS DATE) = d.date
LIMIT 3
""")
for row in cursor.fetchall():
    print(f"  Timestamp: {row[0]}, Event Date: {row[1]}, Dim Date: {row[2]}")

conn.close()

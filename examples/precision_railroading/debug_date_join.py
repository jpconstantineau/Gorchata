import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Check stg_clm_enriched date_id column:")
cursor.execute("SELECT event_id, timestamp, date_id FROM stg_clm_enriched LIMIT 5")
for row in cursor.fetchall():
    print(f"  Event: {row[0]}, Timestamp: {row[1]}, Date ID: {row[2]}")

print("\nTest direct date casting and join:")
cursor.execute("""
SELECT 
    timestamp,
    DATE(timestamp) as date_extracted,
    d.date,
    d.date_id
FROM stg_clm_events e
LEFT JOIN dim_date d ON DATE(e.timestamp) = d.date
LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  TS: {row[0]}, Extracted: {row[1]}, Dim Date: {row[2]}, Date ID: {row[3]}")

conn.close()

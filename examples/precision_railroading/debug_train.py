import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Checking train_id values in raw seed:")
cursor.execute("SELECT DISTINCT train_id FROM raw_clm_events WHERE train_id IS NOT NULL AND train_id != ''")
for row in cursor.fetchall():
    print(f"  Train ID: '{row[0]}'")

print("\nTrains in dim_train:")
cursor.execute("SELECT train_id FROM dim_train")
for row in cursor.fetchall():
    print(f"  Train ID: '{row[0]}'")

print("\nSample events with train_id:")
cursor.execute("SELECT event_id, event_type, train_id FROM stg_clm_events WHERE train_id IS NOT NULL AND TRIM(train_id) != '' LIMIT 5")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]}, Train: '{row[2]}'")

print("\nJoin test:")
cursor.execute("""
SELECT e.event_id, e.train_id, t.train_id, t.train_type
FROM stg_clm_events e
LEFT JOIN dim_train t ON e.train_id = t.train_id
WHERE e.train_id IS NOT NULL AND TRIM(e.train_id) != ''
LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  Event: {row[0]}, Event Train: '{row[1]}', Dim Train: '{row[2]}', Type: {row[3]}")

conn.close()

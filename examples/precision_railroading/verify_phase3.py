import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("=== Phase 3 Data Quality Report ===\n")

print("1. Sample enriched events (first 3):")
cursor.execute("""
SELECT 
    event_id,
    car_number,
    timestamp,
    event_type,
    location_name,
    location_type,
    railroad_owner,
    car_type,
    psr_period,
    is_loaded_event,
    is_movement_event,
    event_sequence
FROM stg_clm_enriched
LIMIT 3
""")
for row in cursor.fetchall():
    print(f"  Event: {row[0]}")
    print(f"    Car: {row[1]}, Type: {row[7]}, Owner: {row[6]}")
    print(f"    Time: {row[2]}, Event Type: {row[3]}, Sequence: {row[11]}")
    print(f"    Location: {row[4]} ({row[5]})")
    print(f"    PSR Period: {row[8]}")
    print(f"    Loaded: {row[9]}, Movement: {row[10]}")
    print()

print("2. Event type distribution:")
cursor.execute("""
SELECT event_type, COUNT(*) as count
FROM stg_clm_enriched
GROUP BY event_type
ORDER BY event_type
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]} events")

print("\n3. Location distribution:")
cursor.execute("""
SELECT location_name, location_type, COUNT(*) as count
FROM stg_clm_enriched
GROUP BY location_name, location_type
ORDER BY count DESC
""")
for row in cursor.fetchall():
    print(f"  {row[0]} ({row[1]}): {row[2]} events")

print("\n4. Event sequence validation (first 5 events per car):")
cursor.execute("""
SELECT car_number, event_sequence, timestamp, event_type
FROM stg_clm_enriched
WHERE car_number IN ('CAR001', 'CAR002')
ORDER BY car_number, event_sequence
LIMIT 10
""")
for row in cursor.fetchall():
    print(f"  {row[0]} - Seq {row[1]}: {row[2]} ({row[3]})")

print("\n5. Dimension join success rates:")
cursor.execute("""
SELECT 
    COUNT(*) as total_events,
    SUM(CASE WHEN location_id IS NOT NULL THEN 1 ELSE 0 END) as with_location,
    SUM(CASE WHEN railcar_id IS NOT NULL THEN 1 ELSE 0 END) as with_railcar,
    SUM(CASE WHEN date_id IS NOT NULL THEN 1 ELSE 0 END) as with_date,
    SUM(CASE WHEN train_db_id IS NOT NULL THEN 1 ELSE 0 END) as with_train
FROM stg_clm_enriched
""")
row = cursor.fetchone()
print(f"  Total events: {row[0]}")
print(f"  Location matches: {row[1]} ({100*row[1]/row[0]:.1f}%)")
print(f"  Railcar matches: {row[2]} ({100*row[2]/row[0]:.1f}%)")
print(f"  Date matches: {row[3]} ({100*row[3]/row[0]:.1f}%)")
print(f"  Train matches: {row[4]} ({100*row[4]/row[0]:.1f}%)")

print("\n6. Temporal ordering check:")
cursor.execute("""
SELECT car_number, COUNT(*) as event_count, MIN(timestamp) as first_event, MAX(timestamp) as last_event
FROM stg_clm_enriched
GROUP BY car_number
ORDER BY car_number
LIMIT 5
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]} events, {row[2]} to {row[3]}")

conn.close()
print("\n=== Report Complete ===")

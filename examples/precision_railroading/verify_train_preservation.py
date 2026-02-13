import sqlite3

conn = sqlite3.connect("target/precision_railroading.db")
cursor = conn.cursor()

print("Check that original train_id is preserved in enriched:")
cursor.execute("""
SELECT event_id, train_id, event_type, train_db_id, train_type
FROM stg_clm_enriched
WHERE train_id IS NOT NULL AND TRIM(train_id) != ''
LIMIT 10
""")
for row in cursor.fetchall():
    print(f"  {row[0]}: Type={row[2]}, Train ID (original)='{row[1]}', Train DB ID={row[3]}, Train Type={row[4]}")

print("\nVerify enriched has correct structure even without train join:")
cursor.execute("""
SELECT 
    COUNT(*) as total,
    COUNT(location_id) as has_location,
    COUNT(railcar_id) as has_railcar,
    COUNT(date_id) as has_date,
    COUNT(DISTINCT car_number) as unique_cars
FROM stg_clm_enriched
""")
row = cursor.fetchone()
print(f"\nSummary:")
print(f"  Total events: {row[0]}")
print(f"  With location: {row[1]}")
print(f"  With railcar: {row[2]}")
print(f"  With date: {row[3]}")
print(f"  Unique cars: {row[4]}")

conn.close()

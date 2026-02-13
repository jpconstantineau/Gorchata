#!/usr/bin/env python3
"""
Build script for Phase 3 staging models
Uses Python's built-in sqlite3 to bypass gorchata infrastructure issues
"""

import sqlite3
import re
import os
from pathlib import Path

def process_template(sql, refs=None):
    """Process gorchata template syntax"""
    if refs is None:
        refs = {}
    
    # Replace {{ config "materialized" "table|view" }} - just remove it
    sql = re.sub(r'\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}', '', sql)
    
    # Replace {{ seed "name" }} with table name
    sql = re.sub(r'\{\{\s*seed\s+"([^"]+)"\s*\}\}', r'\1', sql)
    
    # Replace {{ ref "name" }} with table name
    sql = re.sub(r'\{\{\s*ref\s+"([^"]+)"\s*\}\}', r'\1', sql)
    
    return sql.strip()

def main():
    print("=== Phase 3 Build Script (Python/SQLite3) ===\n")
    
    # Setup paths
    db_path = "target/precision_railroading.db"
    
    # Connect to database
    print(f"Connecting to: {db_path}")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Register custom functions for SQL
    conn.create_function("LEAST", -1, lambda *args: min(args) if args else None)
    conn.create_function("GREATEST", -1, lambda *args: max(args) if args else None)
    
    # Check if we have raw_clm_events
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='raw_clm_events'")
    if not cursor.fetchone():
        print("ERROR: raw_clm_events table not found. Run 'gorchata seed' first.")
        return 1
    
    # Build order: dimensions first, then staging
    models = [
        {
            'name': 'dim_location',
            'path': 'models/dimensions/dim_location.sql',
            'deps': []
        },
        {
            'name': 'dim_railcar',
            'path': 'models/dimensions/dim_railcar.sql',
            'deps': []
        },
        {
            'name': 'dim_train',
            'path': 'models/dimensions/dim_train.sql',
            'deps': []
        },
        {
            'name': 'dim_date',
            'path': 'models/dimensions/dim_date.sql',
            'deps': []
        },
        {
            'name': 'dim_corridor',
            'path': 'models/dimensions/dim_corridor.sql',
            'deps': ['dim_location']
        },
        {
            'name': 'stg_clm_events',
            'path': 'models/staging/stg_clm_events.sql',
            'deps': []
        },
        {
            'name': 'stg_clm_enriched',
            'path': 'models/staging/stg_clm_enriched.sql',
            'deps': ['stg_clm_events', 'dim_location', 'dim_railcar', 'dim_train', 'dim_date']
        }
    ]
    
    print("\nBuilding models...\n")
    
    for model in models:
        print(f"  Building {model['name']}...", end=' ')
        
        # Read SQL file
        sql_path = Path(model['path'])
        if not sql_path.exists():
            print(f"SKIP (file not found at {model['path']})")
            continue
        
        with open(sql_path, 'r') as f:
            raw_sql = f.read()
        
        # Process template
        processed_sql = process_template(raw_sql)
        
        # Drop and create as table
        drop_sql = f"DROP TABLE IF EXISTS {model['name']}"
        create_sql = f"CREATE TABLE {model['name']} AS {processed_sql}"
        
        try:
            cursor.execute(drop_sql)
            cursor.execute(create_sql)
            conn.commit()
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM {model['name']}")
            count = cursor.fetchone()[0]
            print(f"OK ({count} rows)")
        
        except Exception as e:
            print(f"FAIL")
            print(f"    Error: {str(e)}")
            return 1
    
    # Get row counts summary
    print("\n=== Row Counts ===")
    for model in models:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {model['name']}")
            count = cursor.fetchone()[0]
            print(f"  {model['name']}: {count} rows")
        except:
            print(f"  {model['name']}: N/A")
    
    conn.close()
    
    print("\n=== Build Complete ===")
    print(f"Database: {db_path}")
    return 0

if __name__ == '__main__':
    exit(main())

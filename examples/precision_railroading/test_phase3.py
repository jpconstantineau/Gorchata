#!/usr/bin/env python3
"""
Test script for Phase 3 staging models
Uses Python's built-in sqlite3 to bypass gorchata infrastructure issues
"""

import sqlite3
import re
from pathlib import Path

def process_template(sql):
    """Process gorchata template syntax"""
    # Replace {{ config "materialized" "table|view" }}
    sql = re.sub(r'\{\{\s*config\s+"[^"]+"\s+"[^"]+"\s*\}\}', '', sql)
    
    # Replace {{ seed "name" }} with table name
    sql = re.sub(r'\{\{\s*seed\s+"([^"]+)"\s*\}\}', r'\1', sql)
    
    # Replace {{ ref "name" }} with table name
    sql = re.sub(r'\{\{\s*ref\s+"([^"]+)"\s*\}\}', r'\1', sql)
    
    return sql.strip()

def main():
    print("=== Phase 3 Test Script ===\n")
    
    # Setup paths
    db_path = "target/precision_railroading.db"
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Access by column name
    cursor = conn.cursor()
    
    # Register custom functions
    conn.create_function("LEAST", -1, lambda *args: min(args) if args else None)
    conn.create_function("GREATEST", -1, lambda *args: max(args) if args else None)
    
    # Test files to run
    test_files = [
        {
            'name': 'test_stg_clm_events',
            'path': 'tests/staging/test_stg_clm_events.sql'
        },
        {
            'name': 'test_stg_clm_enriched',
            'path': 'tests/staging/test_stg_clm_enriched.sql'
        }
    ]
    
    print("Running tests...\n")
    
    total_tests = 0
    passed_tests = 0
    failed_tests = 0
    
    for test_file in test_files:
        print(f"  Test: {test_file['name']}")
        
        test_path = Path(test_file['path'])
        if not test_path.exists():
            print(f"    SKIP (file not found at {test_file['path']})")
            continue
        
        with open(test_path, 'r') as f:
            raw_sql = f.read()
        
        # Process template
        processed_sql = process_template(raw_sql)
        
        try:
            cursor.execute(processed_sql)
            results = cursor.fetchall()
            
            if len(results) == 0:
                # No violations = test passed
                print(f"    ✓ PASS (no violations)")
                passed_tests += 1
            else:
                # Has violations = test failed
                print(f"    ✗ FAIL ({len(results)} violation(s))")
                failed_tests += 1
                # Print details of violations
                for row in results[:5]:  # Limit to first 5
                    if 'test_name' in row.keys():
                        print(f"      - {row['test_name']}: {row['violation_count']} violations")
                        print(f"        {row['description']}")
                if len(results) > 5:
                    print(f"      ... and {len(results) - 5} more")
            
            total_tests += 1
        
        except Exception as e:
            print(f"    ✗ ERROR: {str(e)}")
            failed_tests += 1
            total_tests += 1
        
        print()
    
    conn.close()
    
    # Summary
    print("=" * 50)
    print(f"Test Summary: {passed_tests}/{total_tests} passed")
    if failed_tests > 0:
        print(f"FAILED: {failed_tests} test(s) failed")
        return 1
    else:
        print("SUCCESS: All tests passed!")
        return 0

if __name__ == '__main__':
    exit(main())

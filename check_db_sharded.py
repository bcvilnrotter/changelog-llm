#!/usr/bin/env python3
"""
Script to check the structure and content of the sharded database.
"""

import sqlite3
import os
import sys
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

from src.db.shard_manager import get_shard_manager

def check_shard(shard_path):
    """Check the structure and content of a single database shard."""
    if not os.path.exists(shard_path):
        print(f"Shard file not found: {shard_path}")
        return
    
    print(f"\n=== Checking shard: {shard_path} ===")
    print(f"Shard size: {os.path.getsize(shard_path) / (1024*1024):.2f} MB")
    
    conn = sqlite3.connect(shard_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables in the shard:")
    for table in tables:
        print(f"- {table[0]}")
    
    # Check entries table
    try:
        cursor.execute("SELECT COUNT(*) FROM entries")
        count = cursor.fetchone()[0]
        print(f"\nEntries table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM entries LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Entries table does not exist or has issues")
    
    # Check training_metadata table
    try:
        cursor.execute("SELECT COUNT(*) FROM training_metadata")
        count = cursor.fetchone()[0]
        print(f"\nTraining metadata table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM training_metadata LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
            
            # Check how many entries have been used in training
            cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
            used_count = cursor.fetchone()[0]
            print(f"Entries used in training: {used_count}")
    except sqlite3.OperationalError:
        print("Training metadata table does not exist or has issues")
    
    # Check token_impacts table
    try:
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        count = cursor.fetchone()[0]
        print(f"\nToken impacts table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM token_impacts LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Token impacts table does not exist or has issues")
    
    # Check token_impact table (singular) - this is the problematic one
    try:
        cursor.execute("SELECT COUNT(*) FROM token_impact")
        count = cursor.fetchone()[0]
        print(f"\nToken impact table (singular): {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM token_impact LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Token impact table (singular) does not exist")
    
    # Check top_tokens table
    try:
        cursor.execute("SELECT COUNT(*) FROM top_tokens")
        count = cursor.fetchone()[0]
        print(f"\nTop tokens table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM top_tokens LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Top tokens table does not exist or has issues")
    
    conn.close()

def check_shard_index(index_path):
    """Check the shard index file."""
    if not os.path.exists(index_path):
        print(f"Shard index file not found: {index_path}")
        return
    
    print(f"\n=== Checking shard index: {index_path} ===")
    print(f"Index size: {os.path.getsize(index_path) / 1024:.2f} KB")
    
    try:
        import json
        with open(index_path, 'r') as f:
            index = json.load(f)
        
        print(f"Number of entries in index: {len(index)}")
        
        # Count entries per shard
        shard_counts = {}
        for page_id, shard_path in index.items():
            if shard_path not in shard_counts:
                shard_counts[shard_path] = 0
            shard_counts[shard_path] += 1
        
        print("Entries per shard:")
        for shard_path, count in shard_counts.items():
            print(f"- {shard_path}: {count} entries")
    
    except Exception as e:
        print(f"Error reading shard index: {str(e)}")

def check_database_sharded(data_dir="data"):
    """Check the structure and content of the sharded database."""
    # Initialize shard manager
    shard_manager = get_shard_manager(data_dir)
    
    # Get all shards
    shard_paths = shard_manager.get_all_shards()
    
    if not shard_paths:
        print(f"No database shards found in {data_dir}")
        
        # Check if legacy database exists
        legacy_db_path = os.path.join(data_dir, "changelog.db")
        if os.path.exists(legacy_db_path):
            print(f"Found legacy database: {legacy_db_path}")
            print("Consider migrating to sharded database using scripts/migrate_to_sharded.py")
            
            # Check legacy database
            check_shard(legacy_db_path)
        
        return
    
    print(f"Found {len(shard_paths)} database shards")
    
    # Check each shard
    for shard_path in shard_paths:
        check_shard(shard_path)
    
    # Check shard index
    index_path = os.path.join(data_dir, "shard_index.json")
    check_shard_index(index_path)
    
    # Print summary
    print("\n=== Summary ===")
    print(f"Total shards: {len(shard_paths)}")
    total_size = sum(os.path.getsize(shard_path) for shard_path in shard_paths)
    print(f"Total size: {total_size / (1024*1024):.2f} MB")
    
    # Count total entries
    total_entries = 0
    total_used_in_training = 0
    
    for shard_path in shard_paths:
        conn = sqlite3.connect(shard_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute("SELECT COUNT(*) FROM entries")
            count = cursor.fetchone()[0]
            total_entries += count
            
            cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
            used_count = cursor.fetchone()[0]
            total_used_in_training += used_count
        except sqlite3.OperationalError:
            pass
        
        conn.close()
    
    print(f"Total entries: {total_entries}")
    print(f"Total entries used in training: {total_used_in_training}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Check the structure and content of the sharded database"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing the sharded database"
    )
    args = parser.parse_args()
    
    check_database_sharded(args.data_dir)

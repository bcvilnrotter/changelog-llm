#!/usr/bin/env python3
"""
Script to validate token impact data in the database for machine unlearning experiments.
This script:
1. Checks if token impact data is being properly stored
2. Provides statistics on token impact data
3. Helps diagnose issues with token impact data storage
"""

import sqlite3
import logging
import sys
import os
from pathlib import Path
import json
import argparse

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(db_path):
    """Create and return a connection to the SQLite database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

def get_table_schema(conn, table_name):
    """Get the schema of a table."""
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    return cursor.fetchall()

def get_table_count(conn, table_name):
    """Get the number of rows in a table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]

def get_token_impact_stats(db_path="data/changelog.db"):
    """
    Get statistics on token impact data in the database.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        dict: Statistics on token impact data
    """
    try:
        # Connect to the database
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        
        # Check if required tables exist
        token_impacts_exists = check_table_exists(conn, "token_impacts")
        token_impact_exists = check_table_exists(conn, "token_impact")
        top_tokens_exists = check_table_exists(conn, "top_tokens")
        
        stats = {
            "tables": {
                "token_impacts_exists": token_impacts_exists,
                "token_impact_exists": token_impact_exists,
                "top_tokens_exists": top_tokens_exists
            },
            "counts": {},
            "schemas": {},
            "sample_data": {}
        }
        
        # Get counts for each table
        if token_impacts_exists:
            stats["counts"]["token_impacts"] = get_table_count(conn, "token_impacts")
            stats["schemas"]["token_impacts"] = [dict(row) for row in get_table_schema(conn, "token_impacts")]
            
            # Get sample data
            cursor.execute("SELECT * FROM token_impacts LIMIT 5")
            stats["sample_data"]["token_impacts"] = [dict(row) for row in cursor.fetchall()]
        
        if token_impact_exists:
            stats["counts"]["token_impact"] = get_table_count(conn, "token_impact")
            stats["schemas"]["token_impact"] = [dict(row) for row in get_table_schema(conn, "token_impact")]
            
            # Get sample data
            cursor.execute("SELECT * FROM token_impact LIMIT 5")
            stats["sample_data"]["token_impact"] = [dict(row) for row in cursor.fetchall()]
        
        if top_tokens_exists:
            stats["counts"]["top_tokens"] = get_table_count(conn, "top_tokens")
            stats["schemas"]["top_tokens"] = [dict(row) for row in get_table_schema(conn, "top_tokens")]
            
            # Get sample data
            cursor.execute("SELECT * FROM top_tokens LIMIT 5")
            stats["sample_data"]["top_tokens"] = [dict(row) for row in cursor.fetchall()]
        
        # Get training metadata stats
        stats["counts"]["training_metadata"] = get_table_count(conn, "training_metadata")
        
        # Get entries stats
        stats["counts"]["entries"] = get_table_count(conn, "entries")
        
        # Get foreign key relationships
        cursor.execute("PRAGMA foreign_key_list(token_impacts)")
        stats["foreign_keys"] = {
            "token_impacts": [dict(row) for row in cursor.fetchall()]
        }
        
        if top_tokens_exists:
            cursor.execute("PRAGMA foreign_key_list(top_tokens)")
            stats["foreign_keys"]["top_tokens"] = [dict(row) for row in cursor.fetchall()]
        
        # Check for orphaned records
        if token_impacts_exists:
            cursor.execute("""
                SELECT COUNT(*) FROM token_impacts ti
                LEFT JOIN training_metadata tm ON ti.metadata_id = tm.id
                WHERE tm.id IS NULL
            """)
            stats["orphaned"] = {
                "token_impacts": cursor.fetchone()[0]
            }
        
        if top_tokens_exists and token_impacts_exists:
            cursor.execute("""
                SELECT COUNT(*) FROM top_tokens tt
                LEFT JOIN token_impacts ti ON tt.token_impact_id = ti.id
                WHERE ti.id IS NULL
            """)
            stats["orphaned"]["top_tokens"] = cursor.fetchone()[0]
        
        # Check for pages with token impact data
        if token_impacts_exists:
            cursor.execute("""
                SELECT COUNT(DISTINCT e.page_id) FROM entries e
                JOIN training_metadata tm ON e.id = tm.entry_id
                JOIN token_impacts ti ON tm.id = ti.metadata_id
            """)
            stats["pages_with_token_impacts"] = cursor.fetchone()[0]
        
        if top_tokens_exists:
            cursor.execute("""
                SELECT COUNT(DISTINCT e.page_id) FROM entries e
                JOIN training_metadata tm ON e.id = tm.entry_id
                JOIN token_impacts ti ON tm.id = ti.metadata_id
                JOIN top_tokens tt ON ti.id = tt.token_impact_id
            """)
            stats["pages_with_top_tokens"] = cursor.fetchone()[0]
        
        return stats
        
    except Exception as e:
        logger.error(f"Error getting token impact stats: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        return {"error": str(e)}
    finally:
        if conn:
            conn.close()

def validate_token_impact_data(db_path="data/changelog.db"):
    """
    Validate token impact data in the database.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        bool: True if validation passed, False otherwise
    """
    try:
        # Get token impact stats
        stats = get_token_impact_stats(db_path)
        
        # Print stats
        logger.info("Token Impact Data Statistics:")
        logger.info(f"Tables: {stats['tables']}")
        logger.info(f"Counts: {stats['counts']}")
        
        # Check if token_impacts table exists
        if not stats["tables"]["token_impacts_exists"]:
            logger.error("token_impacts table does not exist")
            return False
        
        # Check if top_tokens table exists
        if not stats["tables"]["top_tokens_exists"]:
            logger.error("top_tokens table does not exist")
            return False
        
        # Check if token_impacts table has data
        if stats["counts"].get("token_impacts", 0) == 0:
            logger.error("token_impacts table is empty")
            return False
        
        # Check if top_tokens table has data
        if stats["counts"].get("top_tokens", 0) == 0:
            logger.error("top_tokens table is empty")
            return False
        
        # Check for orphaned records
        if stats.get("orphaned", {}).get("token_impacts", 0) > 0:
            logger.error(f"Found {stats['orphaned']['token_impacts']} orphaned token_impacts records")
            return False
        
        if stats.get("orphaned", {}).get("top_tokens", 0) > 0:
            logger.error(f"Found {stats['orphaned']['top_tokens']} orphaned top_tokens records")
            return False
        
        # Check for pages with token impact data
        if stats.get("pages_with_token_impacts", 0) == 0:
            logger.error("No pages have token impact data")
            return False
        
        if stats.get("pages_with_top_tokens", 0) == 0:
            logger.error("No pages have top tokens data")
            return False
        
        logger.info("Token impact data validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating token impact data: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        return False

def export_token_impact_data(db_path="data/changelog.db", output_path="token_impact_data.json"):
    """
    Export token impact data to a JSON file.
    
    Args:
        db_path: Path to the database file
        output_path: Path to save the JSON file
    
    Returns:
        bool: True if export was successful, False otherwise
    """
    try:
        # Connect to the database
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        
        # Get all pages with token impact data
        cursor.execute("""
            SELECT e.page_id, e.title, ti.id as token_impact_id, ti.total_tokens
            FROM entries e
            JOIN training_metadata tm ON e.id = tm.entry_id
            JOIN token_impacts ti ON tm.id = ti.metadata_id
        """)
        
        pages = []
        for row in cursor.fetchall():
            page = dict(row)
            token_impact_id = page.pop("token_impact_id")
            
            # Get top tokens for this token impact
            cursor.execute("""
                SELECT token_id, position, impact, context_start, context_end
                FROM top_tokens
                WHERE token_impact_id = ?
            """, (token_impact_id,))
            
            top_tokens = []
            for token_row in cursor.fetchall():
                token = dict(token_row)
                token["context"] = [token.pop("context_start"), token.pop("context_end")]
                top_tokens.append(token)
            
            page["top_tokens"] = top_tokens
            pages.append(page)
        
        # Save to JSON file
        with open(output_path, "w") as f:
            json.dump(pages, f, indent=2)
        
        logger.info(f"Exported token impact data for {len(pages)} pages to {output_path}")
        return True
        
    except Exception as e:
        logger.error(f"Error exporting token impact data: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Validate token impact data in the database for machine unlearning experiments"
    )
    parser.add_argument(
        "--db-path",
        default="data/changelog.db",
        help="Path to the changelog database"
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export token impact data to a JSON file"
    )
    parser.add_argument(
        "--output",
        default="token_impact_data.json",
        help="Path to save the exported JSON file"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging"
    )
    args = parser.parse_args()
    
    # Set debug level if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Check if database file exists
    if not os.path.exists(args.db_path):
        logger.error(f"Database file not found: {args.db_path}")
        return
    
    # Get token impact stats
    stats = get_token_impact_stats(args.db_path)
    
    # Print stats in a readable format
    print("\nToken Impact Data Statistics:")
    print("============================")
    print(f"Tables:")
    for table, exists in stats["tables"].items():
        print(f"  {table}: {'Exists' if exists else 'Does not exist'}")
    
    print("\nRow Counts:")
    for table, count in stats["counts"].items():
        print(f"  {table}: {count} rows")
    
    if "pages_with_token_impacts" in stats:
        print(f"\nPages with token impact data: {stats['pages_with_token_impacts']}")
    
    if "pages_with_top_tokens" in stats:
        print(f"Pages with top tokens data: {stats['pages_with_top_tokens']}")
    
    if "orphaned" in stats:
        print("\nOrphaned Records:")
        for table, count in stats["orphaned"].items():
            print(f"  {table}: {count} orphaned records")
    
    # Validate token impact data
    print("\nValidating token impact data...")
    valid = validate_token_impact_data(args.db_path)
    
    if valid:
        print("\nToken impact data validation PASSED")
        
        # Export token impact data if requested
        if args.export:
            print(f"\nExporting token impact data to {args.output}...")
            export_token_impact_data(args.db_path, args.output)
    else:
        print("\nToken impact data validation FAILED")
        print("Please run scripts/fix_token_impact_tables.py to fix the database schema")

if __name__ == "__main__":
    main()

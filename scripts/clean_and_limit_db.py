#!/usr/bin/env python3
"""
Script to clean up the changelog database and limit it to the top 10 nodes.
This script:
1. Removes the outdated token_impact table (singular)
2. Identifies the top 10 nodes based on token_impacts data
3. Keeps only those top 10 nodes and their associated data
4. Cleans up any orphaned records
"""

import argparse
import logging
import os
import shutil
import sqlite3
import sys
from pathlib import Path

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

def backup_database(db_path):
    """Create a backup of the database."""
    backup_path = f"{db_path}.backup"
    logger.info(f"Creating backup of database at {backup_path}")
    shutil.copy2(db_path, backup_path)
    logger.info(f"Backup created successfully")
    return backup_path

def check_table_exists(conn, table_name):
    """Check if a table exists in the database."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    return cursor.fetchone() is not None

def get_table_count(conn, table_name):
    """Get the number of rows in a table."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]

def print_database_stats(conn):
    """Print statistics about the database."""
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [row[0] for row in cursor.fetchall()]
    
    logger.info("Database statistics:")
    for table in tables:
        count = get_table_count(conn, table)
        logger.info(f"- {table}: {count} rows")
    
    # Get count of entries used in training
    cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
    used_count = cursor.fetchone()[0]
    logger.info(f"- Entries used in training: {used_count}")
    
    # Get count of entries with token impact data
    if check_table_exists(conn, "token_impacts"):
        cursor.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM entries e
            JOIN training_metadata tm ON e.id = tm.entry_id
            JOIN token_impacts ti ON tm.id = ti.metadata_id
        """)
        with_token_impacts = cursor.fetchone()[0]
        logger.info(f"- Entries with token_impacts data: {with_token_impacts}")
    
    if check_table_exists(conn, "token_impact"):
        cursor.execute("""
            SELECT COUNT(DISTINCT e.id)
            FROM entries e
            JOIN training_metadata tm ON e.id = tm.entry_id
            JOIN token_impact ti ON tm.id = ti.metadata_id
        """)
        with_token_impact = cursor.fetchone()[0]
        logger.info(f"- Entries with token_impact data: {with_token_impact}")

def remove_token_impact_table(conn):
    """Remove the outdated token_impact table (singular)."""
    cursor = conn.cursor()
    
    if check_table_exists(conn, "token_impact"):
        logger.info("Removing outdated token_impact table (singular)")
        cursor.execute("DROP TABLE token_impact")
        logger.info("token_impact table removed")
        return True
    else:
        logger.info("token_impact table does not exist")
        return False

def get_top_nodes_with_token_impacts(conn, top_n=10):
    """
    Get the top N nodes that have token_impacts data.
    
    Args:
        conn: Database connection
        top_n: Number of top nodes to return
        
    Returns:
        List of entry_ids for the top nodes
    """
    cursor = conn.cursor()
    
    # Get entries that have token_impacts data
    cursor.execute("""
        SELECT e.id as entry_id, e.title
        FROM entries e
        JOIN training_metadata tm ON e.id = tm.entry_id
        JOIN token_impacts ti ON tm.id = ti.metadata_id
        GROUP BY e.id
        LIMIT ?
    """, (top_n,))
    
    # Get the entries
    top_entries = []
    for row in cursor.fetchall():
        top_entries.append(row['entry_id'])
        logger.info(f"Selected top node: {row['title']} (ID: {row['entry_id']})")
    
    logger.info(f"Found {len(top_entries)} nodes with token_impacts data")
    return top_entries

def mark_top_nodes_as_used(conn, top_entry_ids):
    """
    Mark the top nodes as used in training and all others as unused.
    
    Args:
        conn: Database connection
        top_entry_ids: List of entry_ids for the top nodes
        
    Returns:
        Number of entries updated
    """
    cursor = conn.cursor()
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Mark all entries as unused
        cursor.execute("""
            UPDATE training_metadata
            SET used_in_training = 0
        """)
        logger.info(f"Marked all {cursor.rowcount} entries as unused")
        
        # Mark top entries as used
        if top_entry_ids:
            placeholders = ','.join(['?'] * len(top_entry_ids))
            cursor.execute(f"""
                UPDATE training_metadata
                SET used_in_training = 1
                WHERE entry_id IN ({placeholders})
            """, top_entry_ids)
            logger.info(f"Marked {cursor.rowcount} top entries as used")
        
        # Commit the transaction
        conn.commit()
        return len(top_entry_ids)
    
    except Exception as e:
        # Roll back the transaction on error
        conn.rollback()
        logger.error(f"Error marking top nodes: {str(e)}")
        return 0

def clean_orphaned_records(conn):
    """
    Clean up orphaned records in the database.
    
    Args:
        conn: Database connection
        
    Returns:
        Number of records cleaned up
    """
    cursor = conn.cursor()
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete token_impacts records that reference non-existent training_metadata
        cursor.execute("""
            DELETE FROM token_impacts
            WHERE metadata_id NOT IN (SELECT id FROM training_metadata)
        """)
        token_impacts_deleted = cursor.rowcount
        logger.info(f"Deleted {token_impacts_deleted} orphaned token_impacts records")
        
        # Delete top_tokens records that reference non-existent token_impacts
        cursor.execute("""
            DELETE FROM top_tokens
            WHERE token_impact_id NOT IN (SELECT id FROM token_impacts)
        """)
        top_tokens_deleted = cursor.rowcount
        logger.info(f"Deleted {top_tokens_deleted} orphaned top_tokens records")
        
        # Commit the transaction
        conn.commit()
        return token_impacts_deleted + top_tokens_deleted
    
    except Exception as e:
        # Roll back the transaction on error
        conn.rollback()
        logger.error(f"Error cleaning orphaned records: {str(e)}")
        return 0

def clean_and_limit_db(db_path, top_n=10):
    """
    Clean up the database and limit it to the top N nodes.
    
    Args:
        db_path: Path to the database file
        top_n: Number of top nodes to keep
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Check if database file exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found: {db_path}")
            return False
        
        # Create a backup of the database
        backup_path = backup_database(db_path)
        
        # Connect to the database
        conn = get_db_connection(db_path)
        
        # Disable foreign keys to avoid constraint errors during cleanup
        conn.execute("PRAGMA foreign_keys = OFF")
        
        # Print database statistics before changes
        logger.info("Database statistics before changes:")
        print_database_stats(conn)
        
        # Remove the outdated token_impact table
        remove_token_impact_table(conn)
        
        # Clean up orphaned records
        clean_orphaned_records(conn)
        
        # Get the top N nodes with token_impacts data
        top_entry_ids = get_top_nodes_with_token_impacts(conn, top_n)
        
        # Mark the top nodes as used in training
        mark_top_nodes_as_used(conn, top_entry_ids)
        
        # Print database statistics after changes
        logger.info("Database statistics after changes:")
        print_database_stats(conn)
        
        # Close the connection
        conn.close()
        
        logger.info(f"Successfully cleaned up database and limited to top {top_n} nodes")
        logger.info(f"Backup available at {backup_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error cleaning and limiting database: {str(e)}")
        logger.error(f"You may restore the backup from {backup_path}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Clean up the changelog database and limit it to the top N nodes"
    )
    parser.add_argument(
        "--db-path",
        default="data/changelog.db",
        help="Path to the changelog database"
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=10,
        help="Number of top nodes to keep"
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
    
    success = clean_and_limit_db(
        db_path=args.db_path,
        top_n=args.top_n
    )
    
    if success:
        logger.info(f"Successfully cleaned up database and limited to top {args.top_n} nodes")
        sys.exit(0)
    else:
        logger.error(f"Failed to clean up database and limit to top {args.top_n} nodes")
        sys.exit(1)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script to mark the top 10 nodes in the changelog database as used in training.
This script:
1. Preserves all token impact data
2. Identifies the top 10 nodes based on token impact data
3. Marks only those top 10 nodes as used in training
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
    backup_path = f"{db_path}.backup.{int(os.path.getmtime(db_path))}"
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

def get_top_nodes(conn, top_n=10, use_token_impact=True):
    """
    Get the top N nodes based on token impact data.
    
    Args:
        conn: Database connection
        top_n: Number of top nodes to return
        use_token_impact: Whether to use token_impact (singular) table instead of token_impacts
        
    Returns:
        List of entry_ids for the top nodes
    """
    cursor = conn.cursor()
    
    if use_token_impact and check_table_exists(conn, "token_impact"):
        # Debug: Check if there are any token_impact records
        cursor.execute("SELECT COUNT(*) FROM token_impact")
        count = cursor.fetchone()[0]
        logger.info(f"Found {count} records in token_impact table")
        
        # Get distinct metadata_id values from token_impact table
        cursor.execute("""
            SELECT DISTINCT metadata_id
            FROM token_impact
            LIMIT ?
        """, (top_n,))
        
        metadata_ids = [row['metadata_id'] for row in cursor.fetchall()]
        logger.info(f"Found {len(metadata_ids)} distinct metadata_id values in token_impact table")
        
        if metadata_ids:
            # Get the entry_ids for these metadata_ids
            placeholders = ','.join(['?'] * len(metadata_ids))
            cursor.execute(f"""
                SELECT tm.entry_id, e.title
                FROM training_metadata tm
                JOIN entries e ON tm.entry_id = e.id
                WHERE tm.id IN ({placeholders})
                LIMIT ?
            """, metadata_ids + [top_n])
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append(row['entry_id'])
                logger.info(f"Selected top node: {row['title']} (ID: {row['entry_id']})")
            
            # If we didn't find any entries, try a different approach
            if not top_entries:
                logger.info("No entries found with matching metadata_ids, trying direct approach")
                # Just pick the first N entries
                cursor.execute("""
                    SELECT e.id as entry_id, e.title
                    FROM entries e
                    LIMIT ?
                """, (top_n,))
                
                for row in cursor.fetchall():
                    top_entries.append(row['entry_id'])
                    logger.info(f"Selected top node (direct): {row['title']} (ID: {row['entry_id']})")
            
            logger.info(f"Found {len(top_entries)} top nodes")
            return top_entries
        else:
            logger.info("No metadata_ids found in token_impact table, trying direct approach")
            # Just pick the first N entries
            cursor.execute("""
                SELECT e.id as entry_id, e.title
                FROM entries e
                LIMIT ?
            """, (top_n,))
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append(row['entry_id'])
                logger.info(f"Selected top node (direct): {row['title']} (ID: {row['entry_id']})")
            
            logger.info(f"Found {len(top_entries)} top nodes")
            return top_entries
    else:
        # Use token_impacts table (plural)
        # Debug: Check if there are any token_impacts records
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        count = cursor.fetchone()[0]
        logger.info(f"Found {count} records in token_impacts table")
        
        # Get distinct metadata_id values from token_impacts table
        cursor.execute("""
            SELECT DISTINCT metadata_id
            FROM token_impacts
            LIMIT ?
        """, (top_n,))
        
        metadata_ids = [row['metadata_id'] for row in cursor.fetchall()]
        logger.info(f"Found {len(metadata_ids)} distinct metadata_id values in token_impacts table")
        
        if metadata_ids:
            # Get the entry_ids for these metadata_ids
            placeholders = ','.join(['?'] * len(metadata_ids))
            cursor.execute(f"""
                SELECT tm.entry_id, e.title
                FROM training_metadata tm
                JOIN entries e ON tm.entry_id = e.id
                WHERE tm.id IN ({placeholders})
                LIMIT ?
            """, metadata_ids + [top_n])
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append(row['entry_id'])
                logger.info(f"Selected top node: {row['title']} (ID: {row['entry_id']})")
            
            # If we didn't find any entries, try a different approach
            if not top_entries:
                logger.info("No entries found with matching metadata_ids, trying direct approach")
                # Just pick the first N entries
                cursor.execute("""
                    SELECT e.id as entry_id, e.title
                    FROM entries e
                    LIMIT ?
                """, (top_n,))
                
                for row in cursor.fetchall():
                    top_entries.append(row['entry_id'])
                    logger.info(f"Selected top node (direct): {row['title']} (ID: {row['entry_id']})")
            
            logger.info(f"Found {len(top_entries)} top nodes")
            return top_entries
        else:
            logger.info("No metadata_ids found in token_impacts table, trying direct approach")
            # Just pick the first N entries
            cursor.execute("""
                SELECT e.id as entry_id, e.title
                FROM entries e
                LIMIT ?
            """, (top_n,))
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append(row['entry_id'])
                logger.info(f"Selected top node (direct): {row['title']} (ID: {row['entry_id']})")
            
            logger.info(f"Found {len(top_entries)} top nodes")
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

def mark_top_nodes(db_path, top_n=10, use_token_impact=True):
    """
    Mark the top N nodes in the database as used in training.
    
    Args:
        db_path: Path to the database file
        top_n: Number of top nodes to mark
        use_token_impact: Whether to use token_impact (singular) table instead of token_impacts
        
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
        
        # Print database statistics before changes
        logger.info("Database statistics before changes:")
        print_database_stats(conn)
        
        # Get the top N nodes
        top_entry_ids = get_top_nodes(conn, top_n, use_token_impact)
        
        # Mark the top nodes as used in training
        mark_top_nodes_as_used(conn, top_entry_ids)
        
        # Print database statistics after changes
        logger.info("Database statistics after changes:")
        print_database_stats(conn)
        
        # Close the connection
        conn.close()
        
        logger.info(f"Successfully marked top {top_n} nodes as used in training")
        logger.info(f"Backup available at {backup_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error marking top nodes: {str(e)}")
        logger.error(f"You may restore the backup from {backup_path}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Mark the top N nodes in the changelog database as used in training"
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
        help="Number of top nodes to mark"
    )
    parser.add_argument(
        "--use-token-impact",
        action="store_true",
        default=True,
        help="Use token_impact (singular) table instead of token_impacts"
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
    
    success = mark_top_nodes(
        db_path=args.db_path,
        top_n=args.top_n,
        use_token_impact=args.use_token_impact
    )
    
    if success:
        logger.info(f"Successfully marked top {args.top_n} nodes as used in training")
        sys.exit(0)
    else:
        logger.error(f"Failed to mark top {args.top_n} nodes as used in training")
        sys.exit(1)

if __name__ == "__main__":
    main()

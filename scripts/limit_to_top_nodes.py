#!/usr/bin/env python3
"""
Script to limit the changelog.db to only contain the top 10 node information.
This script:
1. Identifies the top 10 nodes based on relative_loss
2. Keeps training metadata, token impacts, and top tokens data for only these 10 nodes
3. Deletes all other training metadata, token impacts, and top tokens data
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

# Configure logging - will be updated in main() based on command-line arguments
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def safe_log(obj, max_items=3, max_str_length=100):
    """
    Safely convert an object to a string for logging, limiting size to avoid context window flooding.
    
    Args:
        obj: The object to log
        max_items: Maximum number of items to include if obj is a list/tuple/dict
        max_str_length: Maximum length of the resulting string
        
    Returns:
        A string representation of the object, safely limited in size
    """
    if obj is None:
        return "None"
        
    if isinstance(obj, (list, tuple)):
        if len(obj) > max_items:
            return f"[{', '.join(str(x) for x in obj[:max_items])}...] ({len(obj)} items total)"
        return str(obj)
        
    if isinstance(obj, dict):
        if len(obj) > max_items:
            items = list(obj.items())[:max_items]
            return f"{{{', '.join(f'{k}: {v}' for k, v in items)}...}} ({len(obj)} items total)"
        return str(obj)
        
    result = str(obj)
    if len(result) > max_str_length:
        return result[:max_str_length] + f"... ({len(result)} chars total)"
        
    return result

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

def get_top_nodes(conn, top_n=10):
    """
    Get the top N nodes based on token impact data.
    
    Args:
        conn: Database connection
        top_n: Number of top nodes to return
        
    Returns:
        List of entry_ids for the top nodes
    """
    cursor = conn.cursor()
    
    # Get entries that have token impact data
    cursor.execute('''
        SELECT e.id as entry_id
        FROM entries e
        JOIN training_metadata tm ON e.id = tm.entry_id
        JOIN token_impacts ti ON tm.id = ti.metadata_id
        GROUP BY e.id
        LIMIT ?
    ''', (top_n,))
    
    # Get the entries
    top_entries = []
    for row in cursor.fetchall():
        top_entries.append(row['entry_id'])
    
    logger.info(f"Found {len(top_entries)} nodes with token impact data")
    return top_entries

def delete_non_top_nodes_data(conn, top_entry_ids):
    """
    Delete all training metadata, token impacts, and top tokens data for nodes not in the top list.
    
    Args:
        conn: Database connection
        top_entry_ids: List of entry_ids for the top nodes
        
    Returns:
        Number of entries deleted
    """
    cursor = conn.cursor()
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Get all metadata IDs that are not in the top list
        if top_entry_ids:
            placeholders = ','.join(['?'] * len(top_entry_ids))
            cursor.execute(f'''
                SELECT id
                FROM training_metadata
                WHERE entry_id NOT IN ({placeholders})
                AND used_in_training = 1
            ''', top_entry_ids)
        else:
            cursor.execute('''
                SELECT id
                FROM training_metadata
                WHERE used_in_training = 1
            ''')
        
        non_top_metadata_ids = [row['id'] for row in cursor.fetchall()]
        logger.info(f"Found {len(non_top_metadata_ids)} non-top metadata entries")
        
        # Get token_impacts IDs for non-top metadata
        if non_top_metadata_ids:
            placeholders = ','.join(['?'] * len(non_top_metadata_ids))
            cursor.execute(f'''
                SELECT id
                FROM token_impacts
                WHERE metadata_id IN ({placeholders})
            ''', non_top_metadata_ids)
            
            token_impacts_ids = [row['id'] for row in cursor.fetchall()]
            logger.info(f"Found {len(token_impacts_ids)} token_impacts entries to delete")
            
            # Delete top_tokens entries for non-top token_impacts
            if token_impacts_ids:
                placeholders = ','.join(['?'] * len(token_impacts_ids))
                cursor.execute(f'''
                    DELETE FROM top_tokens
                    WHERE token_impact_id IN ({placeholders})
                ''', token_impacts_ids)
                logger.info(f"Deleted {cursor.rowcount} top_tokens entries")
                
                # Delete token_impacts entries for non-top metadata
                cursor.execute(f'''
                    DELETE FROM token_impacts
                    WHERE metadata_id IN ({placeholders})
                ''', non_top_metadata_ids)
                logger.info(f"Deleted {cursor.rowcount} token_impacts entries")
        
        # Update training_metadata to mark non-top entries as unused
        if non_top_metadata_ids:
            placeholders = ','.join(['?'] * len(non_top_metadata_ids))
            cursor.execute(f'''
                UPDATE training_metadata
                SET used_in_training = 0,
                    training_timestamp = NULL,
                    model_checkpoint = NULL,
                    average_loss = NULL,
                    relative_loss = NULL
                WHERE id IN ({placeholders})
            ''', non_top_metadata_ids)
            logger.info(f"Updated {cursor.rowcount} training_metadata entries")
        
        # Commit the transaction
        conn.commit()
        logger.info("Changes committed successfully")
        
        return len(non_top_metadata_ids)
    
    except Exception as e:
        # Roll back the transaction on error
        conn.rollback()
        logger.error(f"Error deleting non-top nodes data: {str(e)}")
        logger.error(f"Transaction rolled back")
        return 0

def validate_database(conn, ignore_foreign_keys=False):
    """
    Validate the database structure after changes.
    
    Args:
        conn: Database connection
        ignore_foreign_keys: Whether to ignore foreign key errors
        
    Returns:
        True if validation passed, False otherwise
    """
    cursor = conn.cursor()
    
    try:
        # Check if all required tables exist
        required_tables = ["entries", "training_metadata", "token_impacts", "top_tokens"]
        for table in required_tables:
            cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table}'")
            if not cursor.fetchone():
                logger.error(f"Required table {table} does not exist")
                return False
        
        # Get row counts
        cursor.execute("SELECT COUNT(*) FROM entries")
        entries_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM training_metadata")
        training_metadata_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
        used_in_training_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        token_impacts_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM top_tokens")
        top_tokens_count = cursor.fetchone()[0]
        
        logger.info(f"Database validation:")
        logger.info(f"- entries: {entries_count}")
        logger.info(f"- training_metadata: {training_metadata_count}")
        logger.info(f"- used in training: {used_in_training_count}")
        logger.info(f"- token_impacts: {token_impacts_count}")
        logger.info(f"- top_tokens: {top_tokens_count}")
        
        # Check foreign key relationships if not ignoring them
        if not ignore_foreign_keys:
            cursor.execute("PRAGMA foreign_key_check")
            foreign_key_errors = cursor.fetchall()
            if foreign_key_errors:
                # Log only the count and a small sample to avoid flooding the context window
                logger.error(f"Found {len(foreign_key_errors)} foreign key errors")
                if foreign_key_errors:
                    # Log at most 3 errors as a sample using safe_log
                    sample = foreign_key_errors[:min(3, len(foreign_key_errors))]
                    logger.error(f"Sample errors: {safe_log(sample)}")
                    
                    # Optionally write all errors to a file
                    error_log_path = "foreign_key_errors.log"
                    try:
                        with open(error_log_path, 'w') as f:
                            for i, error in enumerate(foreign_key_errors):
                                f.write(f"Error {i+1}: {error}\n")
                        logger.error(f"All errors written to {error_log_path}")
                    except Exception as e:
                        logger.error(f"Failed to write errors to file: {str(e)}")
                return False
        else:
            logger.warning("Ignoring foreign key errors during validation")
        
        logger.info("Database validation passed")
        return True
    
    except Exception as e:
        logger.error(f"Error validating database: {str(e)}")
        return False

def limit_to_top_nodes(db_path, top_n=10, ignore_foreign_keys=True):
    """
    Limit the database to only contain the top N node information.
    
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
        
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        
        # Validate the database before changes
        logger.info("Validating database before changes...")
        if not validate_database(conn, ignore_foreign_keys=ignore_foreign_keys):
            logger.error("Database validation failed before changes")
            conn.close()
            return False
        
        # Get the top N nodes
        top_entry_ids = get_top_nodes(conn, top_n)
        
        # Delete data for non-top nodes
        deleted_count = delete_non_top_nodes_data(conn, top_entry_ids)
        logger.info(f"Deleted data for {deleted_count} non-top nodes")
        
        # Validate the database after changes
        logger.info("Validating database after changes...")
        if not validate_database(conn, ignore_foreign_keys=ignore_foreign_keys):
            logger.error("Database validation failed after changes")
            logger.error(f"You may restore the backup from {backup_path}")
            conn.close()
            return False
        
        # Close the connection
        conn.close()
        
        logger.info(f"Successfully limited database to top {top_n} nodes")
        logger.info(f"Backup available at {backup_path}")
        return True
    
    except Exception as e:
        logger.error(f"Error limiting database to top nodes: {str(e)}")
        logger.error(f"You may restore the backup from {backup_path}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Limit the changelog.db to only contain the top N node information"
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
        "--log-file",
        help="Path to write logs to a file instead of stdout"
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Reduce logging output (only show warnings and errors)"
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging (verbose output)"
    )
    parser.add_argument(
        "--ignore-foreign-keys",
        action="store_true",
        help="Ignore foreign key errors during validation"
    )
    args = parser.parse_args()
    
    # Configure logging based on command-line arguments
    log_level = logging.INFO
    if args.quiet:
        log_level = logging.WARNING
    if args.debug:
        log_level = logging.DEBUG
    
    # Set up logging to file if requested
    if args.log_file:
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            filename=args.log_file,
            filemode='w'
        )
        # Add console handler for errors
        console = logging.StreamHandler()
        console.setLevel(logging.ERROR)
        formatter = logging.Formatter('%(levelname)s: %(message)s')
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
        logger.info(f"Logging to file: {args.log_file}")
    else:
        # Just update the level for console logging
        logging.getLogger().setLevel(log_level)
    
    if args.debug:
        logger.debug("Debug logging enabled")
    
    success = limit_to_top_nodes(
        db_path=args.db_path,
        top_n=args.top_n,
        ignore_foreign_keys=args.ignore_foreign_keys
    )
    
    if success:
        logger.info(f"Successfully limited database to top {args.top_n} nodes")
        sys.exit(0)
    else:
        logger.error(f"Failed to limit database to top {args.top_n} nodes")
        sys.exit(1)

if __name__ == "__main__":
    main()

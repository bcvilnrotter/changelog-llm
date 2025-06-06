#!/usr/bin/env python3
"""
Script to mark the top N nodes in the sharded changelog database as used in training.
This script:
1. Works with the sharded database approach
2. Preserves all token impact data
3. Identifies the top N nodes based on token impact data
4. Marks only those top N nodes as used in training
"""

import argparse
import logging
import os
import shutil
import sqlite3
import sys
import json
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

from src.db.shard_manager import get_shard_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection(db_path):
    """Create and return a connection to the SQLite database."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def backup_database_shard(shard_path):
    """Create a backup of a database shard."""
    backup_path = f"{shard_path}.backup.{int(os.path.getmtime(shard_path))}"
    logger.info(f"Creating backup of shard at {backup_path}")
    shutil.copy2(shard_path, backup_path)
    logger.info(f"Backup created successfully")
    return backup_path

def backup_shard_index(index_path):
    """Create a backup of the shard index."""
    backup_path = f"{index_path}.backup.{int(os.path.getmtime(index_path))}"
    logger.info(f"Creating backup of shard index at {backup_path}")
    shutil.copy2(index_path, backup_path)
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

def print_database_stats(shard_paths):
    """Print statistics about the sharded database."""
    logger.info("Database statistics:")
    
    total_entries = 0
    total_used_in_training = 0
    total_with_token_impacts = 0
    total_with_token_impact = 0
    
    for shard_path in shard_paths:
        conn = get_db_connection(shard_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        logger.info(f"Shard: {shard_path}")
        for table in tables:
            count = get_table_count(conn, table)
            logger.info(f"  - {table}: {count} rows")
        
        # Get count of entries
        cursor.execute("SELECT COUNT(*) FROM entries")
        entries_count = cursor.fetchone()[0]
        total_entries += entries_count
        
        # Get count of entries used in training
        cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
        used_count = cursor.fetchone()[0]
        total_used_in_training += used_count
        logger.info(f"  - Entries used in training: {used_count}")
        
        # Get count of entries with token impact data
        if check_table_exists(conn, "token_impacts"):
            cursor.execute("""
                SELECT COUNT(DISTINCT e.id)
                FROM entries e
                JOIN training_metadata tm ON e.id = tm.entry_id
                JOIN token_impacts ti ON tm.id = ti.metadata_id
            """)
            with_token_impacts = cursor.fetchone()[0]
            total_with_token_impacts += with_token_impacts
            logger.info(f"  - Entries with token_impacts data: {with_token_impacts}")
        
        if check_table_exists(conn, "token_impact"):
            cursor.execute("""
                SELECT COUNT(DISTINCT e.id)
                FROM entries e
                JOIN training_metadata tm ON e.id = tm.entry_id
                JOIN token_impact ti ON tm.id = ti.metadata_id
            """)
            with_token_impact = cursor.fetchone()[0]
            total_with_token_impact += with_token_impact
            logger.info(f"  - Entries with token_impact data: {with_token_impact}")
        
        conn.close()
    
    logger.info("Total statistics across all shards:")
    logger.info(f"  - Total entries: {total_entries}")
    logger.info(f"  - Total entries used in training: {total_used_in_training}")
    logger.info(f"  - Total entries with token_impacts data: {total_with_token_impacts}")
    logger.info(f"  - Total entries with token_impact data: {total_with_token_impact}")

def get_top_nodes_from_shard(conn, top_n=10, use_token_impact=True):
    """
    Get the top N nodes based on token impact data from a single shard.
    
    Args:
        conn: Database connection
        top_n: Number of top nodes to return
        use_token_impact: Whether to use token_impact (singular) table instead of token_impacts
        
    Returns:
        List of (entry_id, title, score) tuples for the top nodes
    """
    cursor = conn.cursor()
    
    if use_token_impact and check_table_exists(conn, "token_impact"):
        # Debug: Check if there are any token_impact records
        cursor.execute("SELECT COUNT(*) FROM token_impact")
        count = cursor.fetchone()[0]
        logger.debug(f"Found {count} records in token_impact table")
        
        # Get distinct metadata_id values from token_impact table
        cursor.execute("""
            SELECT DISTINCT metadata_id
            FROM token_impact
            LIMIT ?
        """, (top_n,))
        
        metadata_ids = [row['metadata_id'] for row in cursor.fetchall()]
        logger.debug(f"Found {len(metadata_ids)} distinct metadata_id values in token_impact table")
        
        if metadata_ids:
            # Get the entry_ids for these metadata_ids
            placeholders = ','.join(['?'] * len(metadata_ids))
            cursor.execute(f"""
                SELECT tm.entry_id, e.title, e.page_id, COUNT(ti.id) as score
                FROM training_metadata tm
                JOIN entries e ON tm.entry_id = e.id
                JOIN token_impact ti ON tm.id = ti.metadata_id
                WHERE tm.id IN ({placeholders})
                GROUP BY tm.entry_id
                ORDER BY score DESC
                LIMIT ?
            """, metadata_ids + [top_n])
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append((row['entry_id'], row['title'], row['page_id'], row['score']))
                logger.debug(f"Selected top node: {row['title']} (ID: {row['entry_id']}, Score: {row['score']})")
            
            return top_entries
        else:
            logger.debug("No metadata_ids found in token_impact table")
            return []
    else:
        # Use token_impacts table (plural)
        # Debug: Check if there are any token_impacts records
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        count = cursor.fetchone()[0]
        logger.debug(f"Found {count} records in token_impacts table")
        
        # Get distinct metadata_id values from token_impacts table
        cursor.execute("""
            SELECT DISTINCT metadata_id
            FROM token_impacts
            LIMIT ?
        """, (top_n,))
        
        metadata_ids = [row['metadata_id'] for row in cursor.fetchall()]
        logger.debug(f"Found {len(metadata_ids)} distinct metadata_id values in token_impacts table")
        
        if metadata_ids:
            # Get the entry_ids for these metadata_ids
            placeholders = ','.join(['?'] * len(metadata_ids))
            cursor.execute(f"""
                SELECT tm.entry_id, e.title, e.page_id, COUNT(ti.id) as score
                FROM training_metadata tm
                JOIN entries e ON tm.entry_id = e.id
                JOIN token_impacts ti ON tm.id = ti.metadata_id
                WHERE tm.id IN ({placeholders})
                GROUP BY tm.entry_id
                ORDER BY score DESC
                LIMIT ?
            """, metadata_ids + [top_n])
            
            top_entries = []
            for row in cursor.fetchall():
                top_entries.append((row['entry_id'], row['title'], row['page_id'], row['score']))
                logger.debug(f"Selected top node: {row['title']} (ID: {row['entry_id']}, Score: {row['score']})")
            
            return top_entries
        else:
            logger.debug("No metadata_ids found in token_impacts table")
            return []

def get_top_nodes(shard_paths, top_n=10, use_token_impact=True):
    """
    Get the top N nodes based on token impact data across all shards.
    
    Args:
        shard_paths: List of paths to database shards
        top_n: Number of top nodes to return
        use_token_impact: Whether to use token_impact (singular) table instead of token_impacts
        
    Returns:
        Dictionary mapping shard paths to lists of entry_ids for the top nodes
    """
    all_top_entries = []
    
    # Collect top entries from all shards
    for shard_path in shard_paths:
        conn = get_db_connection(shard_path)
        top_entries = get_top_nodes_from_shard(conn, top_n, use_token_impact)
        conn.close()
        
        for entry in top_entries:
            all_top_entries.append((entry[0], entry[1], entry[2], entry[3], shard_path))
    
    # Sort all entries by score
    all_top_entries.sort(key=lambda x: x[3], reverse=True)
    
    # Take the top N entries
    top_entries = all_top_entries[:top_n]
    
    # Group by shard
    shard_to_entries = {}
    for entry_id, title, page_id, score, shard_path in top_entries:
        if shard_path not in shard_to_entries:
            shard_to_entries[shard_path] = []
        shard_to_entries[shard_path].append(entry_id)
        logger.info(f"Selected top node: {title} (ID: {entry_id}, Score: {score}, Shard: {shard_path})")
    
    # If we didn't find enough entries, try a different approach
    if not top_entries:
        logger.info("No entries found with token impact data, trying direct approach")
        
        # Just pick the first N entries from each shard
        for shard_path in shard_paths:
            conn = get_db_connection(shard_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT e.id as entry_id, e.title, e.page_id
                FROM entries e
                LIMIT ?
            """, (top_n,))
            
            shard_to_entries[shard_path] = []
            for row in cursor.fetchall():
                shard_to_entries[shard_path].append(row['entry_id'])
                logger.info(f"Selected top node (direct): {row['title']} (ID: {row['entry_id']}, Shard: {shard_path})")
            
            conn.close()
    
    logger.info(f"Found {sum(len(entries) for entries in shard_to_entries.values())} top nodes across {len(shard_to_entries)} shards")
    return shard_to_entries

def mark_top_nodes_as_used(shard_to_entries):
    """
    Mark the top nodes as used in training and all others as unused.
    
    Args:
        shard_to_entries: Dictionary mapping shard paths to lists of entry_ids for the top nodes
        
    Returns:
        Number of entries updated
    """
    total_updated = 0
    
    # Process each shard
    for shard_path, entry_ids in shard_to_entries.items():
        conn = get_db_connection(shard_path)
        cursor = conn.cursor()
        
        # Start a transaction
        conn.execute("BEGIN TRANSACTION")
        
        try:
            # Mark all entries as unused
            cursor.execute("""
                UPDATE training_metadata
                SET used_in_training = 0
            """)
            logger.info(f"Marked all {cursor.rowcount} entries as unused in shard {shard_path}")
            
            # Mark top entries as used
            if entry_ids:
                placeholders = ','.join(['?'] * len(entry_ids))
                cursor.execute(f"""
                    UPDATE training_metadata
                    SET used_in_training = 1
                    WHERE entry_id IN ({placeholders})
                """, entry_ids)
                logger.info(f"Marked {cursor.rowcount} top entries as used in shard {shard_path}")
                total_updated += cursor.rowcount
            
            # Commit the transaction
            conn.commit()
        
        except Exception as e:
            # Roll back the transaction on error
            conn.rollback()
            logger.error(f"Error marking top nodes in shard {shard_path}: {str(e)}")
        
        finally:
            conn.close()
    
    return total_updated

def mark_top_nodes_sharded(data_dir="data", top_n=10, use_token_impact=True):
    """
    Mark the top N nodes in the sharded database as used in training.
    
    Args:
        data_dir: Directory containing the sharded database
        top_n: Number of top nodes to mark
        use_token_impact: Whether to use token_impact (singular) table instead of token_impacts
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Initialize shard manager
        shard_manager = get_shard_manager(data_dir)
        
        # Get all shards
        shard_paths = shard_manager.get_all_shards()
        
        if not shard_paths:
            logger.error(f"No database shards found in {data_dir}")
            return False
        
        logger.info(f"Found {len(shard_paths)} database shards")
        
        # Create backups of all shards
        backup_paths = []
        for shard_path in shard_paths:
            backup_path = backup_database_shard(shard_path)
            backup_paths.append(backup_path)
        
        # Backup shard index
        index_path = Path(data_dir) / "shard_index.json"
        if index_path.exists():
            backup_index_path = backup_shard_index(index_path)
        
        # Print database statistics before changes
        logger.info("Database statistics before changes:")
        print_database_stats(shard_paths)
        
        # Get the top N nodes
        shard_to_entries = get_top_nodes(shard_paths, top_n, use_token_impact)
        
        # Mark the top nodes as used in training
        total_updated = mark_top_nodes_as_used(shard_to_entries)
        
        # Print database statistics after changes
        logger.info("Database statistics after changes:")
        print_database_stats(shard_paths)
        
        logger.info(f"Successfully marked {total_updated} top nodes as used in training")
        logger.info(f"Backups available at: {', '.join(backup_paths)}")
        return True
    
    except Exception as e:
        logger.error(f"Error marking top nodes: {str(e)}")
        logger.error(f"You may restore the backups from: {', '.join(backup_paths)}")
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Mark the top N nodes in the sharded changelog database as used in training"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing the sharded database"
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
    
    success = mark_top_nodes_sharded(
        data_dir=args.data_dir,
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

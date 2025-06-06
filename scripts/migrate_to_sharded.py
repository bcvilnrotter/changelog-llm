#!/usr/bin/env python3
"""
Script to migrate from a single changelog.db to a sharded database setup.
This script will:
1. Create a new sharded database structure
2. Copy all data from the existing database to the new sharded structure
3. Update the shard index

This version is optimized for Google Drive with:
- Smaller batch sizes with pauses to allow sync
- Checkpoint-based migration to resume after failures
- Retry logic for database operations
- Progress tracking and reporting
"""

import argparse
import json
import logging
import os
import sys
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

from src.db.db_schema import init_db
from src.db.shard_manager import get_shard_manager, ShardedChangelogDB

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def get_entry_count(db_path: str) -> int:
    """
    Get the number of entries in the database.
    
    Args:
        db_path: Path to the database file
        
    Returns:
        Number of entries in the database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM entries")
    count = cursor.fetchone()[0]
    conn.close()
    return count

def get_entries_batch(db_path: str, offset: int, limit: int) -> List[Dict]:
    """
    Get a batch of entries from the database.
    
    Args:
        db_path: Path to the database file
        offset: Offset to start from
        limit: Maximum number of entries to return
        
    Returns:
        List of entries
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get entries with their training metadata
    cursor.execute('''
        SELECT e.*, tm.id as metadata_id, tm.used_in_training, tm.training_timestamp, 
               tm.model_checkpoint, tm.average_loss, tm.relative_loss
        FROM entries e
        LEFT JOIN training_metadata tm ON e.id = tm.entry_id
        ORDER BY e.id
        LIMIT ? OFFSET ?
    ''', (limit, offset))
    
    entries = [dict(row) for row in cursor.fetchall()]
    
    # Get token impact data for each entry
    for entry in entries:
        metadata_id = entry.get('metadata_id')
        if metadata_id:
            # Get token impact data
            cursor.execute('''
                SELECT id, total_tokens
                FROM token_impacts
                WHERE metadata_id = ?
            ''', (metadata_id,))
            
            token_impact_row = cursor.fetchone()
            if token_impact_row:
                token_impact_id = token_impact_row['id']
                total_tokens = token_impact_row['total_tokens']
                
                # Get top tokens
                cursor.execute('''
                    SELECT token_id, position, impact, context_start, context_end
                    FROM top_tokens
                    WHERE token_impact_id = ?
                ''', (token_impact_id,))
                
                top_tokens = []
                for token_row in cursor.fetchall():
                    top_tokens.append({
                        "token_id": token_row['token_id'],
                        "position": token_row['position'],
                        "impact": token_row['impact'],
                        "context": [token_row['context_start'], token_row['context_end']]
                    })
                
                entry["token_impact"] = {
                    "top_tokens": top_tokens,
                    "total_tokens": total_tokens
                }
    
    conn.close()
    return entries

def insert_entry_to_shard(entry: Dict, shard_manager: ShardedChangelogDB) -> None:
    """
    Insert an entry into the appropriate shard.
    
    Args:
        entry: Entry to insert
        shard_manager: Shard manager instance
    """
    # Get the shard for writing
    shard_path = shard_manager.get_shard_for_writing()
    
    # Connect to the shard
    conn = sqlite3.connect(shard_path)
    cursor = conn.cursor()
    
    # Check if entry with this page_id already exists
    page_id = entry['page_id']
    if isinstance(page_id, bytes):
        page_id = page_id.decode('utf-8')
    
    cursor.execute('SELECT id FROM entries WHERE page_id = ?', (page_id,))
    existing = cursor.fetchone()
    
    if existing:
        # Skip this entry
        logger.debug(f"Skipping duplicate entry with page_id {page_id}")
        conn.close()
        return
    
    # Insert the entry
    cursor.execute('''
        INSERT INTO entries (
            id, title, page_id, revision_id, timestamp, content_hash,
            action, is_revision, parent_id, revision_number
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        entry['id'],
        entry['title'],
        entry['page_id'],
        entry['revision_id'],
        entry['timestamp'],
        entry['content_hash'],
        entry['action'],
        entry['is_revision'],
        entry['parent_id'],
        entry['revision_number']
    ))
    
    # Insert training metadata
    cursor.execute('''
        INSERT INTO training_metadata (
            id, entry_id, used_in_training, training_timestamp,
            model_checkpoint, average_loss, relative_loss
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', (
        entry['metadata_id'],
        entry['id'],
        entry['used_in_training'],
        entry['training_timestamp'],
        entry['model_checkpoint'],
        entry['average_loss'],
        entry['relative_loss']
    ))
    
    # Insert token impact data if available
    if 'token_impact' in entry:
        token_impact = entry['token_impact']
        
        cursor.execute('''
            INSERT INTO token_impacts (
                metadata_id, total_tokens
            ) VALUES (?, ?)
        ''', (
            entry['metadata_id'],
            token_impact['total_tokens']
        ))
        
        token_impact_id = cursor.lastrowid
        
        # Insert top tokens
        for token in token_impact['top_tokens']:
            context = token['context']
            cursor.execute('''
                INSERT INTO top_tokens (
                    token_impact_id, token_id, position, impact,
                    context_start, context_end
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                token_impact_id,
                token['token_id'],
                token['position'],
                token['impact'],
                context[0],
                context[1]
            ))
    
    # Commit changes
    conn.commit()
    conn.close()
    
    # Update the shard index
    page_id = entry['page_id']
    if isinstance(page_id, bytes):
        page_id = page_id.decode('utf-8')
    shard_manager.shard_index.add_page(page_id, shard_path)

# Constants for retry and pause settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
BATCH_PAUSE = 5  # seconds
LONG_PAUSE = 30  # seconds
LONG_PAUSE_FREQUENCY = 10  # batches

def load_checkpoint(checkpoint_path: str) -> Dict:
    """
    Load migration checkpoint from file.
    
    Args:
        checkpoint_path: Path to the checkpoint file
        
    Returns:
        Checkpoint data as a dictionary
    """
    if os.path.exists(checkpoint_path):
        try:
            with open(checkpoint_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load checkpoint: {str(e)}")
    
    # Return default checkpoint if file doesn't exist or loading fails
    return {
        "last_batch": 0,
        "entries_processed": 0,
        "last_timestamp": datetime.now().isoformat(),
        "processed_page_ids": [],
        "current_shard": ""
    }

def save_checkpoint(checkpoint: Dict, checkpoint_path: str) -> None:
    """
    Save migration checkpoint to file.
    
    Args:
        checkpoint: Checkpoint data as a dictionary
        checkpoint_path: Path to the checkpoint file
    """
    try:
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {str(e)}")

def process_batch(entries: List[Dict], shard_manager: ShardedChangelogDB, checkpoint: Dict) -> int:
    """
    Process a batch of entries with retry logic.
    
    Args:
        entries: List of entries to process
        shard_manager: Shard manager instance
        checkpoint: Checkpoint data
        
    Returns:
        Number of entries successfully processed
    """
    processed_count = 0
    
    for entry in entries:
        # Convert page_id to string if it's bytes
        page_id = entry['page_id']
        if isinstance(page_id, bytes):
            page_id = page_id.decode('utf-8')
        
        # Skip if already processed
        if page_id in checkpoint["processed_page_ids"]:
            logger.debug(f"Skipping already processed entry with page_id {page_id}")
            processed_count += 1
            continue
        
        # Process with retry logic
        success = False
        retries = 0
        
        while not success and retries < MAX_RETRIES:
            try:
                insert_entry_to_shard(entry, shard_manager)
                success = True
                checkpoint["processed_page_ids"].append(page_id)
                processed_count += 1
            except sqlite3.OperationalError as e:
                retries += 1
                logger.warning(f"Retry {retries}/{MAX_RETRIES} for entry {entry['id']}: {str(e)}")
                time.sleep(RETRY_DELAY * retries)  # Exponential backoff
            except Exception as e:
                logger.error(f"Failed to process entry {entry['id']}: {str(e)}")
                break
    
    return processed_count

def migrate_database(source_path: str, target_dir: str, batch_size: int = 50, 
                    shard_size_limit_mb: int = 90, force_restart: bool = False) -> None:
    """
    Migrate from a single database to a sharded setup with checkpoint-based resumption.
    
    Args:
        source_path: Path to the source database file
        target_dir: Directory to store the sharded databases
        batch_size: Number of entries to process in each batch
        shard_size_limit_mb: Maximum size of a shard in megabytes
        force_restart: Whether to force restart from the beginning
    """
    logger.info(f"Migrating database from {source_path} to {target_dir}")
    
    # Create target directory if it doesn't exist
    target_dir_path = Path(target_dir)
    target_dir_path.mkdir(parents=True, exist_ok=True)
    
    # Initialize shard manager
    shard_manager = get_shard_manager(target_dir, shard_size_limit_mb)
    
    # Get total number of entries
    total_entries = get_entry_count(source_path)
    logger.info(f"Found {total_entries} entries in source database")
    
    # Load or initialize checkpoint
    checkpoint_path = os.path.join(target_dir, "migration_checkpoint.json")
    
    if os.path.exists(checkpoint_path) and not force_restart:
        checkpoint = load_checkpoint(checkpoint_path)
        offset = checkpoint["entries_processed"]
        batch_num = checkpoint["last_batch"] + 1
        logger.info(f"Resuming migration from batch {batch_num} (offset {offset})")
    else:
        checkpoint = {
            "last_batch": 0,
            "entries_processed": 0,
            "last_timestamp": datetime.now().isoformat(),
            "processed_page_ids": [],
            "current_shard": ""
        }
        offset = 0
        batch_num = 1
        logger.info("Starting new migration")
    
    # Process entries in batches
    start_time = time.time()
    
    try:
        while offset < total_entries:
            batch_start_time = time.time()
            
            logger.info(f"Processing batch {batch_num}/{(total_entries + batch_size - 1) // batch_size}")
            
            # Get batch of entries
            entries = get_entries_batch(source_path, offset, batch_size)
            
            # Process batch with retry logic
            processed_count = process_batch(entries, shard_manager, checkpoint)
            
            # Update checkpoint
            checkpoint["last_batch"] = batch_num
            checkpoint["entries_processed"] = offset + processed_count
            checkpoint["last_timestamp"] = datetime.now().isoformat()
            checkpoint["current_shard"] = shard_manager.get_shard_for_writing()
            
            # Save checkpoint
            save_checkpoint(checkpoint, checkpoint_path)
            
            # Update offset and batch number
            offset += batch_size
            batch_num += 1
            
            # Calculate progress and ETA
            progress = min(offset, total_entries) / total_entries
            elapsed = time.time() - start_time
            if progress > 0:
                eta = elapsed / progress - elapsed
                eta_str = f"{int(eta // 3600)}h {int((eta % 3600) // 60)}m {int(eta % 60)}s"
            else:
                eta_str = "unknown"
            
            logger.info(f"Processed {min(offset, total_entries)}/{total_entries} entries ({progress:.1%}), ETA: {eta_str}")
            
            # Pause to allow Google Drive to sync
            batch_time = time.time() - batch_start_time
            logger.info(f"Batch processing time: {batch_time:.2f}s")
            
            if batch_num % LONG_PAUSE_FREQUENCY == 0:
                logger.info(f"Taking a longer pause ({LONG_PAUSE}s) to allow full sync...")
                time.sleep(LONG_PAUSE)
            else:
                logger.info(f"Pausing for {BATCH_PAUSE}s to allow sync...")
                time.sleep(BATCH_PAUSE)
    
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        logger.info(f"Progress saved to {checkpoint_path}")
        logger.info("Run the script again to resume from the last successful batch")
        return
    
    # Rebuild the shard index to ensure it's complete
    logger.info("Rebuilding shard index...")
    shard_manager.rebuild_index()
    
    # Clean up checkpoint file after successful completion
    try:
        os.remove(checkpoint_path)
        logger.info("Removed checkpoint file after successful completion")
    except:
        logger.warning(f"Failed to remove checkpoint file: {checkpoint_path}")
    
    logger.info("Migration complete")
    logger.info(f"Created {len(shard_manager.get_all_shards())} shards in {target_dir}")
    logger.info(f"Total time: {time.time() - start_time:.2f}s")

def main():
    global BATCH_PAUSE, LONG_PAUSE
    
    parser = argparse.ArgumentParser(
        description="Migrate from a single changelog.db to a sharded database setup"
    )
    parser.add_argument(
        "--source",
        default="data/changelog.db",
        help="Path to the source database file (default: data/changelog.db)"
    )
    parser.add_argument(
        "--target",
        default="data",
        help="Directory to store the sharded databases (default: data)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Number of entries to process in each batch (default: 50)"
    )
    parser.add_argument(
        "--shard-size-limit",
        type=int,
        default=90,
        help="Maximum size of a shard in megabytes (default: 90)"
    )
    parser.add_argument(
        "--force-restart",
        action="store_true",
        help="Force restart from the beginning, ignoring any existing checkpoint"
    )
    parser.add_argument(
        "--pause",
        type=int,
        default=BATCH_PAUSE,
        help=f"Pause duration in seconds between batches (default: {BATCH_PAUSE})"
    )
    parser.add_argument(
        "--long-pause",
        type=int,
        default=LONG_PAUSE,
        help=f"Long pause duration in seconds every {LONG_PAUSE_FREQUENCY} batches (default: {LONG_PAUSE})"
    )
    args = parser.parse_args()
    
    # Update pause settings if provided
    BATCH_PAUSE = args.pause
    LONG_PAUSE = args.long_pause
    
    try:
        migrate_database(
            source_path=args.source,
            target_dir=args.target,
            batch_size=args.batch_size,
            shard_size_limit_mb=args.shard_size_limit,
            force_restart=args.force_restart
        )
    except KeyboardInterrupt:
        logger.info("Migration interrupted by user")
        logger.info("Run the script again to resume from the last successful batch")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Migration failed: {str(e)}")
        logger.info("Run the script again to resume from the last successful batch")
        sys.exit(1)

if __name__ == "__main__":
    main()

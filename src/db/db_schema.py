"""
SQLite database schema for the changelog-llm project.
This module defines the database schema and provides utility functions
for database initialization.
"""

import json
import os
import sqlite3
from pathlib import Path
from typing import Optional

def get_db_connection(db_path: Optional[str] = None, for_writing: bool = False) -> sqlite3.Connection:
    """
    Create and return a connection to the SQLite database.
    
    Args:
        db_path (str, optional): Path to the database file
        for_writing (bool, optional): Whether the connection is for writing
        
    Returns:
        sqlite3.Connection: A connection to the SQLite database
    """
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        if db_path is None:
            # If no specific path is provided, use the shard manager
            try:
                from src.db.shard_manager import get_shard_manager
                shard_manager = get_shard_manager()
                
                if for_writing:
                    # For writing, use the current shard
                    db_path = shard_manager.get_shard_for_writing()
                    logger.info(f"Using current shard for writing: {db_path}")
                else:
                    # For reading without a specific page_id, use the most recent shard
                    # This is a simplification - in practice, you might need to query multiple shards
                    shards = shard_manager.get_all_shards()
                    if shards:
                        db_path = shards[0]  # Use the first shard (most recent)
                        logger.info(f"Using most recent shard for reading: {db_path}")
                    else:
                        # No shards exist yet, create one
                        db_path = shard_manager.create_new_shard()
                        logger.info(f"No shards exist, created new shard: {db_path}")
            except ImportError:
                # Shard manager not available, fall back to default behavior
                parent_dir = Path(__file__).resolve().parent.parent.parent
                db_path = os.path.join(parent_dir, "data", "changelog.db")
                logger.info(f"Shard manager not available, using default database: {db_path}")
        
        logger.info(f"Opening database connection to: {db_path}")
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # Connect to the database and enable foreign keys
        # Use URI mode to specify encoding parameters
        db_uri = f"file:{db_path}?mode=rwc"
        conn = sqlite3.connect(db_uri, uri=True, detect_types=sqlite3.PARSE_DECLTYPES)
        conn.execute("PRAGMA foreign_keys = ON")
        conn.row_factory = sqlite3.Row
        
        # Configure text factory to handle binary data
        # Use bytes as text factory to avoid encoding issues
        conn.text_factory = bytes
        
        logger.debug("Database connection established successfully")
        return conn
    except Exception as e:
        logger.error(f"Error connecting to database: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise

def init_db(db_path: Optional[str] = None) -> None:
    """
    Initialize the database with the required schema.
    This function creates the necessary tables if they don't exist.
    
    Args:
        db_path (str, optional): Path to the database file
    """
    conn = get_db_connection(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS entries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        page_id TEXT NOT NULL UNIQUE,
        revision_id TEXT NOT NULL,
        timestamp TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        action TEXT NOT NULL,
        is_revision BOOLEAN NOT NULL,
        parent_id TEXT,
        revision_number INTEGER,
        FOREIGN KEY (parent_id) REFERENCES entries (page_id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS training_metadata (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        entry_id INTEGER NOT NULL,
        used_in_training BOOLEAN NOT NULL DEFAULT 0,
        training_timestamp TEXT,
        model_checkpoint TEXT,
        average_loss REAL,
        relative_loss REAL,
        FOREIGN KEY (entry_id) REFERENCES entries (id) ON DELETE CASCADE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS token_impacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        metadata_id INTEGER NOT NULL,
        total_tokens INTEGER NOT NULL,
        FOREIGN KEY (metadata_id) REFERENCES training_metadata (id) ON DELETE CASCADE
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS top_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_impact_id INTEGER NOT NULL,
        token_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        impact REAL NOT NULL,
        context_start INTEGER NOT NULL,
        context_end INTEGER NOT NULL,
        FOREIGN KEY (token_impact_id) REFERENCES token_impacts (id) ON DELETE CASCADE
    )
    ''')
    
    # Create indices for faster querying
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_page_id ON entries (page_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_entries_parent_id ON entries (parent_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_training_metadata_entry_id ON training_metadata (entry_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_token_impacts_metadata_id ON token_impacts (metadata_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_tokens_token_impact_id ON top_tokens (token_impact_id)')
    
    conn.commit()
    conn.close()

if __name__ == "__main__":
    init_db()

#!/usr/bin/env python3
"""
Script to fix the token impact tables in the database for machine unlearning experiments.
This script:
1. Renames token_impact table to token_impacts if needed
2. Ensures proper schema for token_impacts and top_tokens tables
3. Validates the database structure
"""

import sqlite3
import logging
import sys
import os
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Configure logging - set to WARNING to reduce output
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
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

def fix_token_impact_tables(db_path="data/changelog.db"):
    """
    Fix the token impact tables in the database.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        bool: True if fixes were applied, False otherwise
    """
    try:
        # Connect to the database
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        
        # Check if token_impact table exists (singular)
        token_impact_exists = check_table_exists(conn, "token_impact")
        token_impacts_exists = check_table_exists(conn, "token_impacts")
        
        if token_impact_exists and not token_impacts_exists:
            # Reduced logging to prevent context window flooding
            print("Fixing: Converting token_impact to token_impacts")
            
            # Create token_impacts table with correct schema
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS token_impacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metadata_id INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                FOREIGN KEY (metadata_id) REFERENCES training_metadata (id) ON DELETE CASCADE
            )
            ''')
            
            # Copy data from token_impact to token_impacts
            cursor.execute('''
            INSERT INTO token_impacts (id, metadata_id, total_tokens)
            SELECT id, metadata_id, impact as total_tokens
            FROM token_impact
            WHERE token = 'total_tokens'
            ''')
            
            # Create index for token_impacts
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_token_impacts_metadata_id ON token_impacts (metadata_id)')
            
            # Check if top_tokens table exists but is empty
            cursor.execute("SELECT COUNT(*) FROM top_tokens")
            top_tokens_count = cursor.fetchone()[0]
            
            # Commit changes
            conn.commit()
            print("Database schema fixed successfully")
            return True
            
        elif token_impacts_exists:
            print("No changes needed: token_impacts table already exists")
            
            return False
        
        else:
            print("Creating new token_impacts and top_tokens tables")
            
            # Create token_impacts table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS token_impacts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metadata_id INTEGER NOT NULL,
                total_tokens INTEGER NOT NULL,
                FOREIGN KEY (metadata_id) REFERENCES training_metadata (id) ON DELETE CASCADE
            )
            ''')
            
            # Create index for token_impacts
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_token_impacts_metadata_id ON token_impacts (metadata_id)')
            
            # Create top_tokens table if it doesn't exist
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
            
            # Create index for top_tokens
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_top_tokens_token_impact_id ON top_tokens (token_impact_id)')
            
            # Commit changes
            conn.commit()
            print("Database schema created successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error fixing token impact tables: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        return False
    finally:
        if conn:
            conn.close()

def validate_database_structure(db_path="data/changelog.db"):
    """
    Validate the database structure for machine unlearning experiments.
    
    Args:
        db_path: Path to the database file
    
    Returns:
        bool: True if validation passed, False otherwise
    """
    try:
        # Connect to the database
        conn = get_db_connection(db_path)
        cursor = conn.cursor()
        
        print("Validating database structure...")
        
        # Check if all required tables exist
        required_tables = ["entries", "training_metadata", "token_impacts", "top_tokens"]
        for table in required_tables:
            if not check_table_exists(conn, table):
                print(f"Error: Required table {table} does not exist")
                return False
        
        # Get row counts without logging details
        cursor.execute("SELECT COUNT(*) FROM training_metadata")
        training_metadata_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        token_impacts_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM top_tokens")
        top_tokens_count = cursor.fetchone()[0]
        
        print(f"Table counts: training_metadata={training_metadata_count}, token_impacts={token_impacts_count}, top_tokens={top_tokens_count}")
        
        # Check foreign key relationships
        cursor.execute("PRAGMA foreign_key_check")
        foreign_key_errors = cursor.fetchall()
        if foreign_key_errors:
            print(f"Error: Foreign key errors found")
            return False
        
        print("Database structure validation passed")
        return True
        
    except Exception as e:
        logger.error(f"Error validating database structure: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        return False
    finally:
        if conn:
            conn.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fix token impact tables in the database for machine unlearning experiments"
    )
    parser.add_argument(
        "--db-path",
        default="data/changelog.db",
        help="Path to the changelog database"
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate the database structure without making changes"
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
    
    if args.validate_only:
        # Only validate the database structure
        validate_database_structure(args.db_path)
    else:
        # Fix token impact tables
        fixed = fix_token_impact_tables(args.db_path)
        
        # Validate the database structure
        validate_database_structure(args.db_path)

if __name__ == "__main__":
    main()

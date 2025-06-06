#!/usr/bin/env python3
"""
Script to test the sharded database approach.
This script:
1. Creates a test database with multiple shards
2. Adds test data to the shards
3. Verifies that the data can be retrieved correctly
"""

import argparse
import logging
import os
import sys
import random
import string
import time
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
sys.path.insert(0, str(current_dir))

from src.db.changelog_db import ChangelogDB
from src.db.shard_manager import get_shard_manager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def generate_random_content(length=1000):
    """Generate random content for testing."""
    return ''.join(random.choice(string.ascii_letters + string.digits + ' \n') for _ in range(length))

def create_test_data(db, num_entries=1000, content_length=1000):
    """Create test data in the database."""
    logger.info(f"Creating {num_entries} test entries...")
    
    for i in range(num_entries):
        # Generate random data
        title = f"Test Page {i}"
        page_id = f"test_{i}"
        revision_id = f"rev_{i}_1"
        content = generate_random_content(content_length)
        
        # Log the page
        db.log_page(title, page_id, revision_id, content)
        
        # Log progress
        if (i + 1) % 100 == 0:
            logger.info(f"Created {i + 1}/{num_entries} entries")
    
    logger.info(f"Created {num_entries} test entries successfully")

def verify_test_data(db, num_entries=1000):
    """Verify that the test data can be retrieved correctly."""
    logger.info(f"Verifying {num_entries} test entries...")
    
    # Get all pages
    all_pages = db.get_main_pages()
    logger.info(f"Found {len(all_pages)} pages in the database")
    
    # Check that we have at least the expected number of pages
    if len(all_pages) < num_entries:
        logger.error(f"Expected at least {num_entries} pages, but found only {len(all_pages)}")
        return False
    
    # Verify that we can retrieve specific pages
    for i in range(0, num_entries, 100):
        page_id = f"test_{i}"
        history = db.get_page_history(page_id)
        
        if not history:
            logger.error(f"Failed to retrieve page history for {page_id}")
            return False
        
        logger.info(f"Successfully retrieved page history for {page_id}")
    
    logger.info(f"Verified test data successfully")
    return True

def check_shards(data_dir):
    """Check the shards in the database."""
    logger.info(f"Checking shards in {data_dir}...")
    
    # Initialize shard manager
    shard_manager = get_shard_manager(data_dir)
    
    # Get all shards
    shard_paths = shard_manager.get_all_shards()
    logger.info(f"Found {len(shard_paths)} shards")
    
    # Print information about each shard
    for shard_path in shard_paths:
        shard_size = os.path.getsize(shard_path) / (1024 * 1024)
        logger.info(f"Shard: {shard_path}, Size: {shard_size:.2f} MB")
    
    # Check shard index
    index_path = os.path.join(data_dir, "shard_index.json")
    if os.path.exists(index_path):
        index_size = os.path.getsize(index_path) / 1024
        logger.info(f"Shard index: {index_path}, Size: {index_size:.2f} KB")
        
        # Load the index
        import json
        with open(index_path, 'r') as f:
            index = json.load(f)
        logger.info(f"Shard index contains {len(index)} entries")
    else:
        logger.warning(f"Shard index not found: {index_path}")

def test_sharded_db(data_dir="test_data", num_entries=1000, content_length=1000, shard_size_limit_mb=1):
    """Test the sharded database approach."""
    try:
        # Create test directory if it doesn't exist
        os.makedirs(data_dir, exist_ok=True)
        
        # Initialize database with a small shard size limit to force multiple shards
        db = ChangelogDB(data_dir, debug=True, shard_size_limit_mb=shard_size_limit_mb)
        
        # Create test data
        create_test_data(db, num_entries, content_length)
        
        # Check shards
        check_shards(data_dir)
        
        # Verify test data
        success = verify_test_data(db, num_entries)
        
        if success:
            logger.info("Sharded database test completed successfully")
            return True
        else:
            logger.error("Sharded database test failed")
            return False
    
    except Exception as e:
        logger.error(f"Error testing sharded database: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Test the sharded database approach")
    parser.add_argument("--data-dir", default="test_data", help="Directory for test data")
    parser.add_argument("--num-entries", type=int, default=1000, help="Number of test entries to create")
    parser.add_argument("--content-length", type=int, default=1000, help="Length of random content for each entry")
    parser.add_argument("--shard-size-limit", type=int, default=1, help="Maximum size of a shard in megabytes")
    args = parser.parse_args()
    
    success = test_sharded_db(
        data_dir=args.data_dir,
        num_entries=args.num_entries,
        content_length=args.content_length,
        shard_size_limit_mb=args.shard_size_limit
    )
    
    sys.exit(0 if success else 1)

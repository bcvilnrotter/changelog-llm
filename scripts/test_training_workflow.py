#!/usr/bin/env python3
"""
Script to test the daily training workflow on a small number of titles.
This script:
1. Extracts 2-3 titles for training
2. Fetches Wikipedia pages for those titles
3. Trains the model on those pages
4. Runs the mark_top_nodes.py script to limit to the top 10 nodes
5. Checks the database to verify the changes
"""

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_command(command, description=None):
    """Run a command and log the output."""
    if description:
        logger.info(f"Running: {description}")
    
    logger.info(f"Command: {command}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Output: {result.stdout}")
        if result.stderr:
            logger.warning(f"Errors: {result.stderr}")
        return result.stdout
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running command: {e}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Errors: {e.stderr}")
        return None

def extract_titles(count=3):
    """Extract titles for training."""
    logger.info(f"Extracting {count} titles for training")
    
    # Create a small test_titles.json file with a few titles
    titles = ["Artificial intelligence", "Machine learning", "Deep learning"][:count]
    
    with open("test_titles.json", "w") as f:
        json.dump(titles, f)
    
    logger.info(f"Extracted {len(titles)} titles: {titles}")
    return titles

def fetch_wikipedia_pages(titles):
    """Fetch Wikipedia pages for the given titles."""
    logger.info(f"Fetching Wikipedia pages for {len(titles)} titles")
    
    # Save titles to a temporary file
    with open("temp_titles.json", "w") as f:
        json.dump(titles, f)
    
    # Run the fetch_wikipedia.py script with the file
    run_command(f"python scripts/fetch_wikipedia.py --titles @temp_titles.json --debug", 
                "Fetching Wikipedia pages")
    
    # Check if the pages were downloaded
    raw_dir = Path("data/raw")
    if raw_dir.exists():
        files = list(raw_dir.glob("*.txt"))
        logger.info(f"Downloaded {len(files)} files to {raw_dir}")
    else:
        logger.error(f"Directory {raw_dir} does not exist")
    
    # Clean up temporary file
    try:
        os.remove("temp_titles.json")
    except:
        pass

def train_model():
    """Train the model on the downloaded pages."""
    logger.info("Training model")
    
    # Run the train_llm.py script with minimal settings
    run_command(
        "python scripts/train_llm.py "
        "--d-model 64 "  # Smaller model for faster training
        "--num-heads 2 "
        "--num-layers 2 "
        "--max-length 128 "
        "--batch-size 2 "
        "--learning-rate 1e-4 "
        "--num-epochs 1 "  # Just one epoch for testing
        "--min-pages 1 "
        "--debug",
        "Training model"
    )

def limit_to_top_nodes(top_n=10):
    """Limit the database to the top N nodes."""
    logger.info(f"Limiting database to top {top_n} nodes")
    
    # Run the mark_top_nodes.py script
    run_command(f"python scripts/mark_top_nodes.py --top-n {top_n} --debug",
                "Marking top nodes")

def check_database():
    """Check the database to verify the changes."""
    logger.info("Checking database")
    
    # Run the check_db.py script
    output = run_command("python check_db.py", "Checking database")
    
    # Parse the output to get the number of entries used in training
    if output:
        for line in output.splitlines():
            if "Entries used in training:" in line:
                used_count = int(line.split(":")[-1].strip())
                logger.info(f"Found {used_count} entries used in training")
                return used_count
    
    logger.error("Could not determine how many entries are used in training")
    return None

def test_workflow(title_count=3, top_n=10):
    """Test the daily training workflow."""
    try:
        # Step 1: Extract titles
        titles = extract_titles(title_count)
        
        # Step 2: Fetch Wikipedia pages
        fetch_wikipedia_pages(titles)
        
        # Step 3: Train model
        train_model()
        
        # Step 4: Limit to top nodes
        limit_to_top_nodes(top_n)
        
        # Step 5: Check database
        used_count = check_database()
        
        if used_count is not None and used_count == top_n:
            logger.info(f"Test successful! Found {used_count} entries used in training (expected {top_n})")
            return True
        else:
            logger.error(f"Test failed! Found {used_count} entries used in training (expected {top_n})")
            return False
    
    except Exception as e:
        logger.error(f"Error testing workflow: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False

def main():
    parser = argparse.ArgumentParser(
        description="Test the daily training workflow on a small number of titles"
    )
    parser.add_argument(
        "--title-count",
        type=int,
        default=3,
        help="Number of titles to extract"
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
    
    success = test_workflow(
        title_count=args.title_count,
        top_n=args.top_n
    )
    
    if success:
        logger.info("Test completed successfully!")
        sys.exit(0)
    else:
        logger.error("Test failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()

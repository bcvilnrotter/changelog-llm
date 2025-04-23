#!/usr/bin/env python3
"""
Script to prepare the database for machine unlearning experiments.
This script:
1. Fixes the database schema
2. Validates the token impact data
3. Exports token impact data to a JSON file (optional)
"""

import argparse
import logging
import sys
import os
import subprocess
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).resolve().parent
project_root = current_dir.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_name, args=None):
    """
    Run a Python script with the given arguments.
    
    Args:
        script_name: Name of the script to run
        args: List of arguments to pass to the script
    
    Returns:
        int: Return code of the script
    """
    cmd = [sys.executable, os.path.join(current_dir, script_name)]
    if args:
        cmd.extend(args)
    
    logger.info(f"Running: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info(f"Output: {result.stdout}")
        if result.stderr:
            logger.warning(f"Errors: {result.stderr}")
        return result.returncode
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running {script_name}: {e}")
        logger.error(f"Output: {e.stdout}")
        logger.error(f"Errors: {e.stderr}")
        return e.returncode

def main():
    parser = argparse.ArgumentParser(
        description="Prepare the database for machine unlearning experiments"
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
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate the database without making changes"
    )
    args = parser.parse_args()
    
    # Set debug level if requested
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")
    
    # Check if database file exists
    if not os.path.exists(args.db_path):
        logger.error(f"Database file not found: {args.db_path}")
        return 1
    
    # Prepare common arguments
    common_args = ["--db-path", args.db_path]
    if args.debug:
        common_args.append("--debug")
    
    # Step 1: Fix the database schema
    if not args.validate_only:
        logger.info("Step 1: Fixing database schema...")
        fix_args = common_args.copy()
        ret_code = run_script("fix_token_impact_tables.py", fix_args)
        if ret_code != 0:
            logger.error("Failed to fix database schema")
            return ret_code
    
    # Step 2: Validate token impact data
    logger.info("Step 2: Validating token impact data...")
    validate_args = common_args.copy()
    if args.export:
        validate_args.append("--export")
        validate_args.extend(["--output", args.output])
    ret_code = run_script("validate_token_impact_data.py", validate_args)
    if ret_code != 0:
        logger.warning("Token impact data validation failed")
        if args.validate_only:
            logger.info("Run this script without --validate-only to fix the database schema")
        return ret_code
    
    logger.info("Database is now ready for machine unlearning experiments")
    logger.info("See MACHINE_UNLEARNING.md for more information")
    return 0

if __name__ == "__main__":
    sys.exit(main())

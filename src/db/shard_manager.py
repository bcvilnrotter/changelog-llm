"""
Shard manager for the changelog-llm project.
This module provides functionality for managing multiple database shards.
"""

import datetime
import json
import logging
import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from src.db.db_schema import init_db

logger = logging.getLogger(__name__)

class ShardIndex:
    """
    Tracks which pages are in which database shards.
    This class maintains an index mapping page_ids to shard paths.
    """
    
    def __init__(self, index_path: Path):
        """
        Initialize the shard index.
        
        Args:
            index_path: Path to the index file
        """
        self.index_path = index_path
        self.page_to_shard: Dict[str, str] = {}
        self._load_index()
    
    def _load_index(self) -> None:
        """Load the index from disk."""
        if self.index_path.exists():
            try:
                with open(self.index_path, 'r', encoding='utf-8') as f:
                    self.page_to_shard = json.load(f)
                logger.info(f"Loaded shard index with {len(self.page_to_shard)} entries")
            except Exception as e:
                logger.error(f"Error loading shard index: {str(e)}")
                # Initialize with empty index if loading fails
                self.page_to_shard = {}
        else:
            logger.info("No existing shard index found, creating new one")
            self.page_to_shard = {}
    
    def add_page(self, page_id: str, shard_path: str) -> None:
        """
        Add a page to the index.
        
        Args:
            page_id: Wikipedia page ID
            shard_path: Path to the shard containing the page
        """
        # Convert bytes to string if needed
        if isinstance(page_id, bytes):
            page_id = page_id.decode('utf-8')
        
        # Normalize path for consistent storage
        normalized_path = str(Path(shard_path).resolve())
        
        # Only update if the page isn't already in the index or if it's in a different shard
        if page_id not in self.page_to_shard or self.page_to_shard[page_id] != normalized_path:
            self.page_to_shard[page_id] = normalized_path
            # Save after each update to ensure index is always up-to-date
            self.save()
    
    def get_shard_for_page(self, page_id: str) -> Optional[str]:
        """
        Get the shard that contains a specific page.
        
        Args:
            page_id: Wikipedia page ID
            
        Returns:
            Path to the shard containing the page, or None if not found
        """
        # Convert bytes to string if needed
        if isinstance(page_id, bytes):
            page_id = page_id.decode('utf-8')
            
        return self.page_to_shard.get(page_id)
    
    def remove_page(self, page_id: str) -> None:
        """
        Remove a page from the index.
        
        Args:
            page_id: Wikipedia page ID
        """
        # Convert bytes to string if needed
        if isinstance(page_id, bytes):
            page_id = page_id.decode('utf-8')
            
        if page_id in self.page_to_shard:
            del self.page_to_shard[page_id]
            self.save()
    
    def get_pages_in_shard(self, shard_path: str) -> List[str]:
        """
        Get all pages in a specific shard.
        
        Args:
            shard_path: Path to the shard
            
        Returns:
            List of page IDs in the shard
        """
        # Normalize path for consistent comparison
        normalized_path = str(Path(shard_path).resolve())
        
        return [page_id for page_id, path in self.page_to_shard.items() if path == normalized_path]
    
    def save(self) -> None:
        """Save the index to disk."""
        try:
            # Ensure directory exists
            self.index_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.index_path, 'w', encoding='utf-8') as f:
                json.dump(self.page_to_shard, f, indent=2)
            logger.debug(f"Saved shard index with {len(self.page_to_shard)} entries")
        except Exception as e:
            logger.error(f"Error saving shard index: {str(e)}")


class ShardedChangelogDB:
    """
    Manages multiple database shards for the changelog.
    This class handles creating new shards when needed and determining which shard to use.
    """
    
    def __init__(self, base_path: str = "data", shard_size_limit_mb: int = 90):
        """
        Initialize the shard manager.
        
        Args:
            base_path: Base directory for shards
            shard_size_limit_mb: Maximum size of a shard in megabytes
        """
        self.base_path = Path(base_path)
        self.shard_size_limit_bytes = shard_size_limit_mb * 1024 * 1024
        self.index_path = self.base_path / "shard_index.json"
        self.shard_index = ShardIndex(self.index_path)
        self.current_shard_path = self._get_or_create_current_shard()
        
        logger.info(f"Initialized ShardedChangelogDB with base_path={base_path}, "
                   f"shard_size_limit={shard_size_limit_mb}MB")
        logger.info(f"Current shard: {self.current_shard_path}")
    
    def _get_or_create_current_shard(self) -> Path:
        """
        Find the most recent shard or create a new one.
        
        Returns:
            Path to the current shard
        """
        # Check for existing shards
        shards = self._get_all_shard_paths()
        
        if not shards:
            # No existing shards, create a new one
            return self._create_new_shard()
        
        # Get the most recent shard
        most_recent_shard = max(shards, key=lambda p: p.stat().st_mtime)
        
        # Check if it's approaching the size limit
        if self._check_shard_size(most_recent_shard):
            # Create a new shard if the current one is too large
            return self._create_new_shard()
        
        return most_recent_shard
    
    def _get_all_shard_paths(self) -> List[Path]:
        """
        Get paths to all existing shards.
        
        Returns:
            List of paths to all shards
        """
        if not self.base_path.exists():
            return []
        
        # Find all database files that match the shard naming pattern
        return sorted([
            p for p in self.base_path.glob("changelog_*.db")
            if p.name.startswith("changelog_") and p.name.endswith(".db")
        ])
    
    def get_all_shards(self) -> List[str]:
        """
        Get paths to all existing shards as strings.
        
        Returns:
            List of paths to all shards
        """
        return [str(p) for p in self._get_all_shard_paths()]
    
    def _check_shard_size(self, shard_path: Path) -> bool:
        """
        Check if the shard is approaching the size limit.
        
        Args:
            shard_path: Path to the shard
            
        Returns:
            True if the shard is approaching the size limit, False otherwise
        """
        if not shard_path.exists():
            return False
        
        current_size = shard_path.stat().st_size
        is_approaching_limit = current_size >= self.shard_size_limit_bytes
        
        if is_approaching_limit:
            logger.info(f"Shard {shard_path} is approaching size limit "
                       f"({current_size / (1024*1024):.2f}MB/{self.shard_size_limit_bytes / (1024*1024):.2f}MB)")
        
        return is_approaching_limit
    
    def _create_new_shard(self) -> Path:
        """
        Create a new shard.
        
        Returns:
            Path to the new shard
        """
        now = datetime.datetime.now()
        new_shard_name = f"changelog_{now.year}_{now.month:02d}.db"
        new_shard_path = self.base_path / new_shard_name
        
        # Initialize the new shard with the schema
        logger.info(f"Creating new shard: {new_shard_path}")
        init_db(str(new_shard_path))
        
        return new_shard_path
    
    def get_shard_for_writing(self) -> str:
        """
        Get the path to the current shard for writing.
        This will create a new shard if the current one is too large.
        
        Returns:
            Path to the current shard for writing
        """
        # Check if the current shard is approaching the size limit
        if self._check_shard_size(self.current_shard_path):
            self.current_shard_path = self._create_new_shard()
        
        return str(self.current_shard_path)
    
    def get_shard_for_reading(self, page_id: Optional[str] = None) -> List[str]:
        """
        Get the path(s) to the shard(s) for reading.
        If page_id is provided, returns the specific shard containing that page.
        Otherwise, returns all shards.
        
        Args:
            page_id: Optional Wikipedia page ID
            
        Returns:
            List of paths to shards for reading
        """
        if page_id is not None:
            shard_path = self.shard_index.get_shard_for_page(page_id)
            if shard_path and Path(shard_path).exists():
                return [shard_path]
        
        # If no specific page_id or the page wasn't found in the index,
        # return all shards
        return self.get_all_shards()
    
    def should_create_new_shard(self) -> bool:
        """
        Check if a new shard should be created.
        
        Returns:
            True if a new shard should be created, False otherwise
        """
        return self._check_shard_size(self.current_shard_path)
    
    def create_new_shard(self) -> str:
        """
        Create a new shard and set it as the current shard.
        
        Returns:
            Path to the new shard
        """
        self.current_shard_path = self._create_new_shard()
        return str(self.current_shard_path)
    
    def rebuild_index(self) -> None:
        """
        Rebuild the shard index by scanning all shards.
        This is useful if the index becomes corrupted or out of sync.
        """
        logger.info("Rebuilding shard index...")
        
        # Clear the existing index
        self.shard_index.page_to_shard = {}
        
        # Scan all shards
        for shard_path in self._get_all_shard_paths():
            try:
                # Connect to the shard
                conn = sqlite3.connect(str(shard_path))
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                
                # Get all page_ids in this shard
                cursor.execute("SELECT page_id FROM entries")
                
                # Add each page to the index
                for row in cursor.fetchall():
                    page_id = row['page_id']
                    if isinstance(page_id, bytes):
                        page_id = page_id.decode('utf-8')
                    
                    self.shard_index.add_page(page_id, str(shard_path))
                
                conn.close()
                
            except Exception as e:
                logger.error(f"Error scanning shard {shard_path}: {str(e)}")
        
        # Save the rebuilt index
        self.shard_index.save()
        logger.info(f"Shard index rebuilt with {len(self.shard_index.page_to_shard)} entries")


# Singleton instance of the shard manager
_shard_manager_instance = None

def get_shard_manager(base_path: str = "data", shard_size_limit_mb: int = 90) -> ShardedChangelogDB:
    """
    Get the singleton instance of the shard manager.
    
    Args:
        base_path: Base directory for shards
        shard_size_limit_mb: Maximum size of a shard in megabytes
        
    Returns:
        Singleton instance of the shard manager
    """
    global _shard_manager_instance
    
    if _shard_manager_instance is None:
        _shard_manager_instance = ShardedChangelogDB(base_path, shard_size_limit_mb)
    
    return _shard_manager_instance

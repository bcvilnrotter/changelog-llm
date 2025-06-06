# Sharded Database for Changelog-LLM

This document explains the sharded database approach implemented for the Changelog-LLM project to prevent the changelog.db file from exceeding 100MB.

## Overview

The sharded database approach splits the database into multiple smaller database files (shards) when the size approaches a configurable limit (default: 90MB). Each shard has the same schema as the original database, and a master index keeps track of which pages are in which shards.

## How It Works

1. **Shard Manager**: The `ShardedChangelogDB` class manages multiple database shards, creating new ones when needed.
2. **Shard Index**: A JSON file (`shard_index.json`) maps page IDs to shard paths.
3. **Time-based Sharding**: Shards are named `changelog_YYYY_MM.db` based on the date they were created.
4. **Automatic Shard Creation**: When a shard approaches the size limit, a new shard is automatically created.

## Usage

### Migrating from a Single Database

If you have an existing `changelog.db` file, you can migrate it to the sharded approach using the provided migration script:

```bash
python scripts/migrate_to_sharded.py --source data/changelog.db --target data
```

Options:
- `--source`: Path to the source database file (default: data/changelog.db)
- `--target`: Directory to store the sharded databases (default: data)
- `--batch-size`: Number of entries to process in each batch (default: 1000)
- `--shard-size-limit`: Maximum size of a shard in megabytes (default: 90)

### Using the Sharded Database

The sharded database is used automatically by the existing code. The `ChangelogDB` class has been updated to use the sharded approach, so no changes are needed to your code that uses this class.

If you're using the lower-level database functions directly, you should use the updated `get_db_connection` function, which now takes an additional parameter `for_writing`:

```python
from src.db.db_schema import get_db_connection

# For reading
conn = get_db_connection(for_writing=False)

# For writing
conn = get_db_connection(for_writing=True)
```

### GitHub Actions Integration

When using GitHub Actions for training, the sharded database approach will be used automatically. The database files will be stored in the `data` directory, and the shard index will be stored in `data/shard_index.json`.

## Configuration

You can configure the shard size limit when initializing the `ChangelogDB` class:

```python
from src.db.changelog_db import ChangelogDB

# Initialize with a custom shard size limit (in megabytes)
db = ChangelogDB(shard_size_limit_mb=50)
```

## Technical Details

### Shard Manager

The `ShardedChangelogDB` class in `src/db/shard_manager.py` manages the shards:

- `get_shard_for_writing()`: Returns the path to the current shard for writing.
- `get_shard_for_reading(page_id)`: Returns the path(s) to the shard(s) for reading.
- `should_create_new_shard()`: Checks if a new shard should be created.
- `create_new_shard()`: Creates a new shard and sets it as the current shard.
- `rebuild_index()`: Rebuilds the shard index by scanning all shards.

### Shard Index

The `ShardIndex` class in `src/db/shard_manager.py` maintains the mapping between page IDs and shard paths:

- `add_page(page_id, shard_path)`: Adds a page to the index.
- `get_shard_for_page(page_id)`: Gets the shard that contains a specific page.
- `get_pages_in_shard(shard_path)`: Gets all pages in a specific shard.

### Database Operations

The database operations in `src/db/db_utils.py` have been updated to work with the sharded approach:

- For writing operations, the current shard is used.
- For reading operations, the specific shard containing the page is used if known, otherwise all shards are queried.
- The shard index is updated whenever a page is written to or read from a shard.

## Troubleshooting

### Rebuilding the Shard Index

If the shard index becomes corrupted or out of sync, you can rebuild it using the `rebuild_index` method:

```python
from src.db.shard_manager import get_shard_manager

shard_manager = get_shard_manager()
shard_manager.rebuild_index()
```

### Checking Shard Sizes

You can check the sizes of all shards using the following command:

```bash
du -h data/changelog_*.db
```

### Listing All Shards

You can list all shards using the following code:

```python
from src.db.shard_manager import get_shard_manager

shard_manager = get_shard_manager()
shards = shard_manager.get_all_shards()
print(shards)

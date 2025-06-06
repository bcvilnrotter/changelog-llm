---
title: Changelog LLM Chatbot
emoji: 🤖
colorFrom: blue
colorTo: red
sdk: gradio
sdk_version: 3.50.2
python_version: "3.10"
app_file: app.py
pinned: false
---

# Changelog LLM Chatbot

This is a custom transformer model trained on Wikipedia data, focusing on basic concepts and common knowledge.

## Model Details
- Custom transformer architecture
- Trained on curated Wikipedia articles
- Updated weekly with new training data

## Usage
Simply type your message in the chat interface and press enter. The model will generate a response based on its training.

Example queries:
- Tell me about basic physics concepts
- Explain how simple machines work
- What are some common English words?

## Updates
This Space is automatically updated every Sunday at midnight UTC with the latest model weights and improvements.

## Technical Details
- Model is updated weekly from the latest training runs
- Uses a custom transformer architecture
- Trained on carefully curated Wikipedia articles
- Optimized for educational content and basic concepts
- Uses SQLite database for tracking training data and model performance
- Supports machine unlearning experiments (see [MACHINE_UNLEARNING.md](MACHINE_UNLEARNING.md))

## Database System

The training data tracking system uses SQLite with a sharded database approach to prevent database files from exceeding 100MB:

1. **Why SQLite?**
   - Better performance for large datasets
   - Structured query capabilities
   - Transaction support
   - Reduced memory footprint

2. **Sharded Database Approach**
   - Database is split into multiple smaller files (shards) when size approaches 90MB
   - Each shard has the same schema as the original database
   - A master index keeps track of which pages are in which shards
   - See [SHARDED_DATABASE.md](SHARDED_DATABASE.md) for detailed information

3. **Migration Process**
   - Run `python scripts/migrate_to_sharded.py` to convert existing database to sharded approach
   - Run `python migrate_to_sqlite.py` to convert JSON data to SQLite (legacy)
   - All new data is stored in the sharded SQLite database by default

4. **Using the Database**
   - The ChangelogDB class provides a high-level interface that handles sharding automatically
   - For direct database access, use the functions in `db_utils.py`
   - Run `python test_db.py` to verify the database functionality

## Machine Unlearning Support

This project now supports machine unlearning experiments, which allow the model to "forget" specific information it has learned during training.

1. **Token Impact Tracking**
   - The system tracks which tokens have the most impact on the model's predictions
   - This information is stored in the database as "token impact data"
   - Token impact data is essential for targeted machine unlearning

2. **Preparing for Machine Unlearning**
   - Run `python scripts/prepare_for_machine_unlearning.py` to prepare the database
   - This script fixes the database schema and validates token impact data
   - See [MACHINE_UNLEARNING.md](MACHINE_UNLEARNING.md) for detailed information

3. **Next Steps**
   - Implement machine unlearning algorithms that use the token impact data
   - Create evaluation metrics to measure the effectiveness of unlearning
   - Compare different unlearning approaches

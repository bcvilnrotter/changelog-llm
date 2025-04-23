# Machine Unlearning Support for Changelog LLM

This document explains how to prepare the Changelog LLM for machine unlearning experiments by ensuring that token impact data is properly stored in the database.

## Background

Machine unlearning is the process of making a model "forget" specific information it has learned during training. To conduct machine unlearning experiments, we need to track which tokens have the most impact on the model's predictions. This information is stored in the database as "token impact data".

## Issues Identified

The current implementation has several issues that prevent token impact data from being properly stored:

1. **Database Schema Mismatch**: The schema defines a `token_impacts` table (plural), but there's a `token_impact` table (singular) in the database.

2. **Data Structure Mismatch**: In `train_llm.py`, token impact data is collected as a list of dictionaries with "critical_tokens", "impact_threshold", and "total_tokens", but in `db_utils.py`, the `mark_used_in_training` function expects token impact data as a dictionary with "top_tokens" and "total_tokens".

3. **Empty Tables**: The `training_metadata`, `token_impacts`, and `top_tokens` tables are empty, which means no token-specific impact data is being stored.

## Fixes Implemented

The following fixes have been implemented to address these issues:

1. **Database Schema Fix**: A new script `scripts/fix_token_impact_tables.py` has been created to fix the database schema by renaming the `token_impact` table to `token_impacts` if it exists, and ensuring the proper schema for `token_impacts` and `top_tokens` tables.

2. **Data Structure Fix**: The `train_llm.py` file has been modified to format token impact data correctly before passing it to `mark_used_in_training`. Specifically, "critical_tokens" is renamed to "top_tokens" to match the expected format.

3. **Data Storage Fix**: The `db_utils.py` file has been updated to ensure it correctly processes and stores token impact data, with improved error handling and logging.

4. **Validation Script**: A new script `scripts/validate_token_impact_data.py` has been created to validate that token impact data is being properly stored in the database.

## How to Use

### 1. Fix the Database Schema

Run the following command to fix the database schema:

```bash
python scripts/fix_token_impact_tables.py
```

This script will:
- Rename the `token_impact` table to `token_impacts` if it exists
- Ensure the proper schema for `token_impacts` and `top_tokens` tables
- Validate the database structure

### 2. Validate Token Impact Data

Run the following command to validate that token impact data is being properly stored:

```bash
python scripts/validate_token_impact_data.py
```

This script will:
- Check if token impact data is being properly stored
- Provide statistics on token impact data
- Help diagnose issues with token impact data storage

You can also export token impact data to a JSON file for further analysis:

```bash
python scripts/validate_token_impact_data.py --export --output token_impact_data.json
```

### 3. Train the Model

The modified `train_llm.py` script will now correctly format token impact data before passing it to `mark_used_in_training`. Run the training script as usual:

```bash
python scripts/train_llm.py
```

## Token Impact Data Structure

Token impact data is stored in the following tables:

1. **token_impacts**: Stores the total number of tokens for each training metadata entry.
   - `id`: Primary key
   - `metadata_id`: Foreign key to training_metadata.id
   - `total_tokens`: Total number of tokens in the sequence

2. **top_tokens**: Stores information about the most impactful tokens.
   - `id`: Primary key
   - `token_impact_id`: Foreign key to token_impacts.id
   - `token_id`: Token ID in the vocabulary
   - `position`: Position of the token in the sequence
   - `impact`: Impact value of the token
   - `context_start`: Start position of the context window
   - `context_end`: End position of the context window

## Machine Unlearning Experiments

With token impact data properly stored, you can now conduct machine unlearning experiments by:

1. Identifying the most impactful tokens for specific concepts or knowledge
2. Modifying the training data to exclude or alter these tokens
3. Fine-tuning the model on the modified data
4. Evaluating the model's performance to see if it has "forgotten" the targeted information

## Next Steps

1. Implement machine unlearning algorithms that use the token impact data
2. Create evaluation metrics to measure the effectiveness of unlearning
3. Compare different unlearning approaches

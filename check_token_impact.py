import sqlite3

def check_token_impact_tables(db_path="data/changelog.db"):
    """Check the structure and content of the token_impact and token_impacts tables."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Check token_impact table (singular)
    print("=== TOKEN_IMPACT TABLE (SINGULAR) ===")
    # Get schema of token_impact table
    cursor.execute("PRAGMA table_info(token_impact)")
    columns = cursor.fetchall()
    print("Token impact table schema:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    # Get sample data
    cursor.execute("SELECT * FROM token_impact LIMIT 5")
    rows = cursor.fetchall()
    print("\nSample data from token_impact table:")
    for row in rows:
        print(row)
    
    # Get unique tokens
    cursor.execute("SELECT DISTINCT token FROM token_impact")
    tokens = cursor.fetchall()
    print(f"\nUnique tokens: {len(tokens)}")
    print(f"First 10 tokens: {tokens[:10]}")
    
    # Get metadata_id distribution
    cursor.execute("SELECT metadata_id, COUNT(*) FROM token_impact GROUP BY metadata_id LIMIT 10")
    metadata_counts = cursor.fetchall()
    print("\nMetadata ID distribution (first 10):")
    for metadata_id, count in metadata_counts:
        print(f"Metadata ID {metadata_id}: {count} rows")
    
    # Check token_impacts table (plural)
    print("\n\n=== TOKEN_IMPACTS TABLE (PLURAL) ===")
    # Get schema of token_impacts table
    cursor.execute("PRAGMA table_info(token_impacts)")
    columns = cursor.fetchall()
    print("Token impacts table schema:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    # Get sample data
    cursor.execute("SELECT * FROM token_impacts LIMIT 5")
    rows = cursor.fetchall()
    print("\nSample data from token_impacts table:")
    for row in rows:
        print(row)
    
    # Get metadata_id distribution
    cursor.execute("SELECT metadata_id, COUNT(*) FROM token_impacts GROUP BY metadata_id LIMIT 10")
    metadata_counts = cursor.fetchall()
    print("\nMetadata ID distribution (first 10):")
    for metadata_id, count in metadata_counts:
        print(f"Metadata ID {metadata_id}: {count} rows")
    
    # Check top_tokens table
    print("\n\n=== TOP_TOKENS TABLE ===")
    # Get schema of top_tokens table
    cursor.execute("PRAGMA table_info(top_tokens)")
    columns = cursor.fetchall()
    print("Top tokens table schema:")
    for col in columns:
        print(f"- {col[1]} ({col[2]})")
    
    # Get sample data
    cursor.execute("SELECT * FROM top_tokens LIMIT 5")
    rows = cursor.fetchall()
    print("\nSample data from top_tokens table:")
    for row in rows:
        print(row)
    
    # Get token_impact_id distribution
    cursor.execute("SELECT token_impact_id, COUNT(*) FROM top_tokens GROUP BY token_impact_id LIMIT 10")
    impact_counts = cursor.fetchall()
    print("\nToken impact ID distribution (first 10):")
    for impact_id, count in impact_counts:
        print(f"Token impact ID {impact_id}: {count} rows")
    
    conn.close()

if __name__ == "__main__":
    check_token_impact_tables()

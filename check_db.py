import sqlite3
import os

def check_database(db_path="data/changelog.db"):
    """Check the structure and content of the database."""
    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print("Tables in the database:")
    for table in tables:
        print(f"- {table[0]}")
    
    # Check entries table
    try:
        cursor.execute("SELECT COUNT(*) FROM entries")
        count = cursor.fetchone()[0]
        print(f"\nEntries table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM entries LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Entries table does not exist or has issues")
    
    # Check training_metadata table
    try:
        cursor.execute("SELECT COUNT(*) FROM training_metadata")
        count = cursor.fetchone()[0]
        print(f"\nTraining metadata table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM training_metadata LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
            
            # Check how many entries have been used in training
            cursor.execute("SELECT COUNT(*) FROM training_metadata WHERE used_in_training = 1")
            used_count = cursor.fetchone()[0]
            print(f"Entries used in training: {used_count}")
    except sqlite3.OperationalError:
        print("Training metadata table does not exist or has issues")
    
    # Check token_impacts table
    try:
        cursor.execute("SELECT COUNT(*) FROM token_impacts")
        count = cursor.fetchone()[0]
        print(f"\nToken impacts table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM token_impacts LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Token impacts table does not exist or has issues")
    
    # Check token_impact table (singular) - this is the problematic one
    try:
        cursor.execute("SELECT COUNT(*) FROM token_impact")
        count = cursor.fetchone()[0]
        print(f"\nToken impact table (singular): {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM token_impact LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Token impact table (singular) does not exist")
    
    # Check top_tokens table
    try:
        cursor.execute("SELECT COUNT(*) FROM top_tokens")
        count = cursor.fetchone()[0]
        print(f"\nTop tokens table: {count} rows")
        
        if count > 0:
            cursor.execute("SELECT * FROM top_tokens LIMIT 1")
            columns = [description[0] for description in cursor.description]
            print(f"Columns: {columns}")
    except sqlite3.OperationalError:
        print("Top tokens table does not exist or has issues")
    
    conn.close()

if __name__ == "__main__":
    check_database()

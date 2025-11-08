import sqlite3
import time

class MetadataStore:
    def insert_new_file(self, file_id, current_path, current_tier="Hot"):
        """
        Adds a new file record when a file is initially created.
        
        :param file_id: Unique identifier for the file.
        :param current_path: The file's physical location (e.g., in mnt_ssd).
        :param current_tier: The starting tier (default is "Hot").
        :return: True on success, False on duplicate or error.
        """
        # time.time() returns the number of seconds since the epoch (a standard timestamp)
        current_time = time.time()
        
        sql_insert = """
        INSERT INTO files (file_id, current_path, current_tier, last_accessed_timestamp, access_count_last_7_days, created_timestamp)
        VALUES (?, ?, ?, ?, ?, ?);
        """
        try:
            # The '?' placeholders prevent SQL injection and map to the tuple of values below
            self.cursor.execute(sql_insert, (
                file_id, 
                current_path, 
                current_tier, 
                current_time, 
                0,      # Initial access count is 0
                current_time # Creation time is now
            ))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            # Handles the case where we try to insert a file that already exists (file_id is PRIMARY KEY)
            return False 
        except sqlite3.Error as e:
            print(f"Error inserting new file: {e}")
            return False
    """
    Manages the SQLite database for tracking file metadata and access patterns.
    """
    def __init__(self, db_name='tiering_metadata.db'):
        # 1. Store the database file name
        self.db_name = db_name
        self.conn = None # Connection object
        self.cursor = None # Cursor object for executing commands
        
        # Call the setup methods when a MetadataStore object is created
        self._connect()
        self._create_table()

    def _connect(self):
        """Establishes a connection to the SQLite database."""
        try:
            # sqlite3.connect will create the file if it doesn't exist
            self.conn = sqlite3.connect(self.db_name)
            self.cursor = self.conn.cursor()
        except sqlite3.Error as e:
            print(f"Error connecting to database: {e}")

    def _create_table(self):
        """
        Creates the 'files' table with all the required fields.
        This defines what data we track for each file.
        """
        sql_create_table = """
        CREATE TABLE IF NOT EXISTS files (
            file_id TEXT PRIMARY KEY,
            current_path TEXT NOT NULL,
            current_tier TEXT NOT NULL,
            last_accessed_timestamp REAL,
            access_count_last_7_days INTEGER,
            created_timestamp REAL
        );
        """
        try:
            self.cursor.execute(sql_create_table)
            self.conn.commit()
            print("Metadata table checked/created successfully.")
        except sqlite3.Error as e:
            print(f"Error creating table: {e}")

    def get_all_files(self):
        """
        Retrieves all file records. Used by the Tiering Logic Engine.
        """
        sql_select = "SELECT * FROM files;"
        self.cursor.execute(sql_select)
        # Returns a list of tuples (rows)
        return self.cursor.fetchall()
    
    def update_file_stats(self, file_id, last_accessed_time, access_count):
        """
        Updates the access statistics for a specific file.
        Used by the Pattern Analyzer.
        """
        sql_update = """
        UPDATE files 
        SET last_accessed_timestamp = ?, 
            access_count_last_7_days = ?
        WHERE file_id = ?;
        """
        try:
            self.cursor.execute(sql_update, (last_accessed_time, access_count, file_id))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating file stats for {file_id}: {e}")
            return False
        
    def update_file_location(self, file_id, new_path, new_tier):
        """
        Updates the file's current location and tier after a move is executed.
        """
        sql_update = """
        UPDATE files 
        SET current_path = ?, 
            current_tier = ?
        WHERE file_id = ?;
        """
        try:
            self.cursor.execute(sql_update, (new_path, new_tier, file_id))
            self.conn.commit()
            return True
        except sqlite3.Error as e:
            print(f"Error updating file location for {file_id}: {e}")
            return False

    # The rest of the functions (insert_new_file, get_all_files, etc.)
    # will go here in the next sub-steps.
    
    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()

if __name__ == '__main__':
    # Use a dummy path that reflects your Windows structure for testing
    TEST_PATH_BASE = "C:\\Users\\YourUser\\Documents\\storage-tiering-project\\mnt_ssd" 
    
    print("--- Running Metadata Store Insertion Test ---")
    
    # 1. Initialize the store (this connects to tiering_metadata.db)
    store = MetadataStore()
    
    # 2. Insert a simulated new file
    file_id_A = "doc_A_123"
    path_A = f"{TEST_PATH_BASE}\\doc_A.txt"
    print(f"Inserting file: {file_id_A}")
    store.insert_new_file(file_id_A, path_A)

    file_id_B = "doc_B_456"
    path_B = f"{TEST_PATH_BASE}\\doc_B.txt"
    print(f"Inserting file: {file_id_B}")
    store.insert_new_file(file_id_B, path_B)


    # 3. Try to insert the same file again (should fail gracefully)
    print(f"\nAttempting to re-insert {file_id_A}...")
    if not store.insert_new_file(file_id_A, path_A):
        print(f" [OK] Duplicate insert for {file_id_A} blocked by unique constraint.")

    # 4. Verify the records were created (should show 2 unique records)
    all_records = store.get_all_files()
    print(f"\n--- Current Database Records (Expected 2 Unique Files) ---")
    for row in all_records:
        print(row)

    # 5. Cleanup
    store.close()
    print("\nTest complete. Insertion functionality verified.")
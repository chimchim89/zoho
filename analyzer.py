import pandas as pd
import time
from metadata_store import MetadataStore
import os

LOG_FILE = "access_log.csv"
DB_NAME = "tiering_metadata.db"

def analyze_patterns():
    """
    Reads the access log, aggregates access counts, and updates the database.
    """
    if not os.path.exists(LOG_FILE):
        print(f"FATAL ERROR: Log file '{LOG_FILE}' not found. Please run workload_sim.py first.")
        return

    print("--- 1. Reading Access Log and Calculating Counts ---")
    
    # 1. Read the CSV log using pandas
    # The 'timestamp' column is crucial here
    try:
        df = pd.read_csv(LOG_FILE)
    except pd.errors.EmptyDataError:
        print("Log file is empty. Skipping analysis.")
        return

    # Convert timestamp column to numeric (it should be REAL from time.time())
    df['timestamp'] = pd.to_numeric(df['timestamp'])
    
    # 2. Aggregate Data: Count accesses per file_id
    access_counts = df.groupby('file_id').size().reset_index(name='access_count')
    
    # 3. Aggregate Data: Find the most recent access time per file_id
    last_accesses = df.groupby('file_id')['timestamp'].max().reset_index(name='last_access_time')

    # 4. Merge the two aggregates into a single table for processing
    analysis_df = pd.merge(access_counts, last_accesses, on='file_id')

    print(f"Found {len(analysis_df)} unique files in the log to analyze.")
    
    # --- 5. Update Database ---
    print("--- 2. Updating Metadata Store with Analysis Results ---")
    store = MetadataStore(DB_NAME)
    
    update_count = 0
    
    # Iterate through the analysis results and update the database
    for index, row in analysis_df.iterrows():
        file_id = row['file_id']
        access_count = row['access_count']
        last_access = row['last_access_time']
        
        # Call the new function from metadata_store.py
        if store.update_file_stats(file_id, last_access, access_count):
            update_count += 1
        
    store.close()
    
    print(f"\n--- DONE: Successfully updated statistics for {update_count} files. ---")


if __name__ == '__main__':
    analyze_patterns()
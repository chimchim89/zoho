import argparse
import pandas as pd
import time
from metadata_store import MetadataStore
import os

LOG_FILE = "access_log.csv"
DB_NAME = "tiering_metadata.db"


def compute_ewma(previous_score, new_sample, alpha=0.3):
    """Compute EWMA update."""
    if previous_score is None:
        return new_sample
    return alpha * new_sample + (1 - alpha) * previous_score


def analyze_patterns(alpha=0.3):
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
    print("--- 2. Updating Metadata Store with Analysis Results (EWMA pattern scoring) ---")
    store = MetadataStore(DB_NAME)

    update_count = 0

    # Parameters for pattern scoring
    now = time.time()
    MAX_COUNT = analysis_df['access_count'].max() if not analysis_df['access_count'].empty else 1

    # Fetch existing scores to apply EWMA
    existing = {r[0]: r[5] for r in store.get_all_files()}  # file_id -> access_pattern_score

    # Iterate through the analysis results and update the database
    for index, row in analysis_df.iterrows():
        file_id = row['file_id']
        access_count = row['access_count']
        last_access = row['last_access_time']

        # recency_score: linear decay over 7 days
        seconds_since = now - float(last_access)
        recency_score = max(0.0, 1.0 - (seconds_since / (7 * 24 * 3600)))  # 7-day window

        frequency_score = float(access_count) / float(MAX_COUNT) if MAX_COUNT > 0 else 0.0

        # New sample is a combination of recency and frequency
        new_sample = 0.4 * recency_score + 0.6 * frequency_score

        prev_score = existing.get(file_id, 0.0)
        pattern_score = compute_ewma(prev_score, new_sample, alpha=alpha)

        # Store the computed pattern score in the DB
        if store.update_file_stats(file_id, last_access, access_count, pattern_score):
            update_count += 1

    store.close()

    print(f"\n--- DONE: Successfully updated statistics (including EWMA pattern scores) for {update_count} files. ---")


def main():
    parser = argparse.ArgumentParser(description='Analyze access logs and update pattern scores')
    parser.add_argument('--alpha', type=float, default=0.3, help='EWMA alpha for pattern score updates')
    args = parser.parse_args()
    analyze_patterns(alpha=args.alpha)


if __name__ == '__main__':
    main()


if __name__ == '__main__':
    analyze_patterns()
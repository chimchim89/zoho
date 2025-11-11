import argparse
import json
import time
from metadata_store import MetadataStore
import os
import shutil # For local file movement (mv command equivalent)
import boto3 # For S3 interaction

# --- Configuration (UPDATE THIS BLOCK) ---
# Ensure these paths and AWS settings match your setup!

# Local Paths (Adjust to your exact Windows paths)
HOT_TIER_PATH = "C:\\Users\\aacha\\OneDrive\\Desktop\\zoho\\mnt_ssd" 
WARM_TIER_PATH = "C:\\Users\\aacha\\OneDrive\\Desktop\\zoho\\mnt_hdd"
# AWS S3 Settings (Replace with your actual values)
S3_BUCKET_NAME = "my-tiering-cold-storage-2025" # <<--- YOUR BUCKET NAME
AWS_REGION = "us-east-1" 

# Local cloud simulation (for testing without AWS)
# Defaults will be overridden by config.json when provided
USE_LOCAL_CLOUD = True
LOCAL_CLOUD_PATH = "C:\\Users\\aacha\\OneDrive\\Desktop\\zoho\\mnt_cloud"
CONFIG_PATH_DEFAULT = "config.json"

# ... [The rest of the TIERING LOGIC CONFIGURATION remains the same]

# --- 1. Tiering Logic Configuration (Based on your Design Doc) ---

# Time constants in seconds (for easy comparison with time.time() timestamps)
SECONDS_IN_DAY = 24 * 60 * 60
DAYS = SECONDS_IN_DAY 

# Promotion Thresholds (Moving UP to a faster tier)
# If accessed within 24 hours, move from Cold to Warm (can be overridden)
PROMOTE_COLD_TO_WARM_DAYS = 1 

# If accessed more than X times in 7 days, move from Warm to Hot (can be overridden)
PROMOTE_WARM_TO_HOT_COUNT = 10 

# Demotion Thresholds (Moving DOWN to a cheaper tier)
# Defaults (in seconds) - will be overridden from config if present
DEMOTE_HOT_TO_WARM_DAYS = 14 * DAYS
DEMOTE_WARM_TO_COLD_DAYS = 60 * DAYS

# Pattern thresholds (defaults)
PATTERN_PROTECT_THRESHOLD = 0.6
WARM_TO_COLD_PATTERN_BLOCK = 0.5
PROMOTE_PATTERN_THRESHOLD = 0.7

# --- 2. Data Mover Functions ---

def execute_move(move_detail, store):
    """
    Executes the physical/logical data movement based on the move plan.
    """
    file_id = move_detail['id']
    from_tier = move_detail['from']
    to_tier = move_detail['to']
    source_path = move_detail['path']
    file_name = os.path.basename(source_path)
    
    print(f"  [MOVING] {file_id}: {from_tier} -> {to_tier}...")

    # Determine the destination path/key
    if to_tier == 'Hot':
        dest_path = os.path.join(HOT_TIER_PATH, file_name)
    elif to_tier == 'Warm':
        dest_path = os.path.join(WARM_TIER_PATH, file_name)
    elif to_tier == 'Cold':
        # Destination is S3, path is the S3 key
        dest_key = file_name 

    try:
        if to_tier in ['Hot', 'Warm'] and from_tier in ['Hot', 'Warm']:
            # Local-to-Local Move (SSD <-> HDD)
            shutil.move(source_path, dest_path)
            new_path = dest_path
            
        elif to_tier == 'Cold':
            # Local -> Cold (S3 or Local Cloud)
            if USE_LOCAL_CLOUD:
                # Ensure local cloud dir exists
                os.makedirs(LOCAL_CLOUD_PATH, exist_ok=True)
                cloud_dest = os.path.join(LOCAL_CLOUD_PATH, file_name)
                shutil.move(source_path, cloud_dest)
                new_path = cloud_dest
            else:
                # Upload to S3
                s3 = boto3.client('s3', region_name=AWS_REGION)
                s3.upload_file(source_path, S3_BUCKET_NAME, dest_key)
                os.remove(source_path)
                new_path = f"s3://{S3_BUCKET_NAME}/{dest_key}" # Update path to S3 URL
            
        elif from_tier == 'Cold':
            # Cold -> Local (Retrieval)
            if USE_LOCAL_CLOUD:
                cloud_source = os.path.join(LOCAL_CLOUD_PATH, file_name)
                shutil.move(cloud_source, dest_path)
                new_path = dest_path
            else:
                s3 = boto3.client('s3', region_name=AWS_REGION)
                dest_key = file_name
                s3.download_file(S3_BUCKET_NAME, dest_key, dest_path)
                new_path = dest_path 

        # --- UPDATE DATABASE (Critical Step) ---
        if store.update_file_location(file_id, new_path, to_tier):
            print(f"  [SUCCESS] Updated DB. New Location: {new_path}")
            return True
        else:
            print(f"  [ERROR] Move success, but DB update failed for {file_id}.")
            return False

    except Exception as e:
        print(f"  [FATAL MOVE ERROR] {from_tier} -> {to_tier} failed for {file_id}: {e}")
        return False


def generate_move_plan(store=None):
    """
    Applies tiering rules to all files in the database and generates a move plan.
    If `store` is provided, it will be used (useful for tests); otherwise a new MetadataStore is created.
    """
    created_store = False
    if store is None:
        store = MetadataStore()
        created_store = True
    all_files = store.get_all_files()
    if created_store:
        store.close()
    
    # Map column indices from your MetadataStore.get_all_files() query
    # Columns: file_id (0), current_path (1), current_tier (2), last_accessed_timestamp (3), access_count_last_7_days (4), access_pattern_score (5), created_timestamp (6)
    
    move_plan = []
    current_time = time.time()
    
    print(f"--- 1. Applying Tiering Logic to {len(all_files)} Files ---")

    for file_record in all_files:
        # Support older schema without access_pattern_score (6 columns) and new schema (7 columns)
        if len(file_record) == 7:
            file_id, current_path, current_tier, last_access, access_count, pattern_score, _ = file_record
        elif len(file_record) == 6:
            file_id, current_path, current_tier, last_access, access_count, _ = file_record
            pattern_score = 0.0
        else:
            # Unexpected row shape; skip
            print(f"Warning: unexpected DB row shape for record: {file_record}")
            continue
        
        # Calculate time difference in seconds
        time_since_last_access = current_time - last_access if last_access else float('inf')
        
    # --- DEMOTION LOGIC (Moving Down) ---
        
        if current_tier == "Hot":
            # Rule: Hot -> Warm (if not accessed for threshold AND low pattern score)
            # We protect high pattern_score files from demotion even if last access is old
            PATTERN_PROTECT_THRESHOLD = 0.6
            if time_since_last_access > DEMOTE_HOT_TO_WARM_DAYS and (pattern_score < PATTERN_PROTECT_THRESHOLD):
                move_plan.append({
                    'id': file_id,
                    'from': 'Hot',
                    'to': 'Warm',
                    'path': current_path,
                    'reason': f"Unused for > {DEMOTE_HOT_TO_WARM_DAYS / DAYS:.0f} days and low pattern score ({pattern_score:.2f})."
                })
                
        elif current_tier == "Warm":
            # Rule: Warm -> Cold (if not accessed for 60 days) but protect if pattern is high
            WARM_TO_COLD_PATTERN_BLOCK = 0.5
            if time_since_last_access > DEMOTE_WARM_TO_COLD_DAYS and (pattern_score < WARM_TO_COLD_PATTERN_BLOCK):
                move_plan.append({
                    'id': file_id,
                    'from': 'Warm',
                    'to': 'Cold',
                    'path': current_path,
                    'reason': f"Unused for > {DEMOTE_WARM_TO_COLD_DAYS / DAYS:.0f} days and low pattern score ({pattern_score:.2f})."
                })

        # --- PROMOTION LOGIC (Moving Up) ---
        
        # Note: In our current simulation, files start at 'Hot'.
        # This logic is mainly for files that have already been moved down.

        # Use pattern_score to promote as well: high pattern_score in Warm should go Hot
        PROMOTE_PATTERN_THRESHOLD = 0.7
        if current_tier == "Warm" and (access_count > PROMOTE_WARM_TO_HOT_COUNT or pattern_score > PROMOTE_PATTERN_THRESHOLD):
            # Rule: Warm -> Hot (if accessed frequently or pattern indicates hotness)
            move_plan.append({
                'id': file_id,
                'from': 'Warm',
                'to': 'Hot',
                'path': current_path,
                'reason': f"Access count is {access_count} or pattern score {pattern_score:.2f} exceeds thresholds."
            })
            
        elif current_tier == "Cold" and time_since_last_access < PROMOTE_COLD_TO_WARM_DAYS:
            # Rule: Cold -> Warm (if retrieved from archive/accessed recently)
            move_plan.append({
                'id': file_id,
                'from': 'Cold',
                'to': 'Warm',
                'path': current_path,
                'reason': f"Accessed within the last {PROMOTE_COLD_TO_WARM_DAYS} day."
            })
            
    return move_plan

# --- 3. Modify Main Execution Block ---

# Find the 'if __name__ == '__main__': ' block and modify it as follows:

def main(dry_run=False, show_scores=False, use_local_cloud=None, config_path=None):
    global USE_LOCAL_CLOUD
    global DEMOTE_HOT_TO_WARM_DAYS, DEMOTE_WARM_TO_COLD_DAYS
    global PROMOTE_WARM_TO_HOT_COUNT, PROMOTE_COLD_TO_WARM_DAYS
    global PATTERN_PROTECT_THRESHOLD, WARM_TO_COLD_PATTERN_BLOCK, PROMOTE_PATTERN_THRESHOLD
    if use_local_cloud is not None:
        USE_LOCAL_CLOUD = use_local_cloud

    # Load config file if present; config_path param overrides default
    cfg_path = config_path if config_path else CONFIG_PATH_DEFAULT
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, 'r') as f:
                cfg = json.load(f)

            # Apply config overrides (days -> seconds conversion)
            if 'demote_hot_to_warm_days' in cfg:
                DEMOTE_HOT_TO_WARM_DAYS = float(cfg['demote_hot_to_warm_days']) * DAYS
            if 'demote_warm_to_cold_days' in cfg:
                DEMOTE_WARM_TO_COLD_DAYS = float(cfg['demote_warm_to_cold_days']) * DAYS
            if 'promote_cold_to_warm_days' in cfg:
                PROMOTE_COLD_TO_WARM_DAYS = float(cfg['promote_cold_to_warm_days'])
            if 'promote_warm_to_hot_count' in cfg:
                PROMOTE_WARM_TO_HOT_COUNT = int(cfg['promote_warm_to_hot_count'])
            if 'pattern_protect_threshold' in cfg:
                PATTERN_PROTECT_THRESHOLD = float(cfg['pattern_protect_threshold'])
            if 'warm_to_cold_pattern_block' in cfg:
                WARM_TO_COLD_PATTERN_BLOCK = float(cfg['warm_to_cold_pattern_block'])
            if 'promote_pattern_threshold' in cfg:
                PROMOTE_PATTERN_THRESHOLD = float(cfg['promote_pattern_threshold'])
            if 'use_local_cloud' in cfg:
                USE_LOCAL_CLOUD = bool(cfg['use_local_cloud'])
            if 'local_cloud_path' in cfg:
                LOCAL_CLOUD_PATH = cfg['local_cloud_path']

        except Exception as e:
            print(f"Warning: failed to read config.json: {e}. Using defaults.")

    store = MetadataStore()
    plan = generate_move_plan()

    print("\n--- 2. MOVE PLAN GENERATED ---")

    if show_scores:
        print("Current file scores (file_id: pattern_score):")
        for r in store.get_all_files():
            print(f"  {r[0]}: {r[5]:.3f}")

    if plan:
        print(f"Total Moves Recommended: {len(plan)}\n")

        for move in plan:
            print(f"- Plan: {move['id']} {move['from']} -> {move['to']} because {move.get('reason')}")
            if not dry_run:
                execute_move(move, store)

    else:
        print("No moves are currently recommended based on the tiering rules.")

    store.close()
    print("\nTiering Engine execution complete.")


def cli():
    parser = argparse.ArgumentParser(description='Tiering engine: generate and execute move plans')
    parser.add_argument('--dry-run', action='store_true', help='Only generate and print move plan; do not execute moves')
    parser.add_argument('--show-scores', action='store_true', help='Show access pattern scores for all files')
    parser.add_argument('--use-local-cloud', type=str, choices=['true','false'], help='Override local cloud usage')
    parser.add_argument('--config', type=str, help='Path to config.json to override defaults')
    args = parser.parse_args()

    use_local = None
    if args.use_local_cloud is not None:
        use_local = True if args.use_local_cloud.lower() == 'true' else False

    cfg_path = args.config if args.config else None

    main(dry_run=args.dry_run, show_scores=args.show_scores, use_local_cloud=use_local, config_path=cfg_path)


if __name__ == '__main__':
    cli()
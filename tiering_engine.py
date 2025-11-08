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

# ... [The rest of the TIERING LOGIC CONFIGURATION remains the same]

# --- 1. Tiering Logic Configuration (Based on your Design Doc) ---

# Time constants in seconds (for easy comparison with time.time() timestamps)
SECONDS_IN_DAY = 24 * 60 * 60
DAYS = SECONDS_IN_DAY 

# Promotion Thresholds (Moving UP to a faster tier)
# If accessed within 24 hours, move from Cold to Warm
PROMOTE_COLD_TO_WARM_DAYS = 1 

# If accessed more than 10 times in 7 days, move from Warm to Hot
PROMOTE_WARM_TO_HOT_COUNT = 10 

# Demotion Thresholds (Moving DOWN to a cheaper tier)
# If not accessed for 14 days, move from Hot to Warm
DEMOTE_HOT_TO_WARM_DAYS = DEMOTE_HOT_TO_WARM_DAYS = 0.0001 * DAYS  # Forces demotion almost immediately #14 * DAYS

# If not accessed for 60 days, move from Warm to Cold
DEMOTE_WARM_TO_COLD_DAYS = 60 * DAYS

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
            # Local -> S3 (Archive)
            s3 = boto3.client('s3', region_name=AWS_REGION)
            s3.upload_file(source_path, S3_BUCKET_NAME, dest_key)
            
            # Delete the local source file after successful upload
            os.remove(source_path) 
            new_path = f"s3://{S3_BUCKET_NAME}/{dest_key}" # Update path to S3 URL
            
        elif from_tier == 'Cold':
            # S3 -> Local (Retrieval) - Simplified: always retrieves to Warm
            s3 = boto3.client('s3', region_name=AWS_REGION)
            dest_key = file_name
            s3.download_file(S3_BUCKET_NAME, dest_key, dest_path)
            
            # In a real system, you would update the S3 object's tier or delete it.
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


def generate_move_plan():
    """
    Applies tiering rules to all files in the database and generates a move plan.
    """
    store = MetadataStore()
    all_files = store.get_all_files()
    store.close()
    
    # Map column indices from your MetadataStore.get_all_files() query
    # Columns: file_id (0), current_path (1), current_tier (2), last_accessed_timestamp (3), access_count_last_7_days (4), created_timestamp (5)
    
    move_plan = []
    current_time = time.time()
    
    print(f"--- 1. Applying Tiering Logic to {len(all_files)} Files ---")

    for file_record in all_files:
        file_id, current_path, current_tier, last_access, access_count, _ = file_record
        
        # Calculate time difference in seconds
        time_since_last_access = current_time - last_access if last_access else float('inf')
        
        # --- DEMOTION LOGIC (Moving Down) ---
        
        if current_tier == "Hot":
            # Rule: Hot -> Warm (if not accessed for 14 days)
            if time_since_last_access > DEMOTE_HOT_TO_WARM_DAYS:
                move_plan.append({
                    'id': file_id,
                    'from': 'Hot',
                    'to': 'Warm',
                    'path': current_path,
                    'reason': f"Unused for > {DEMOTE_HOT_TO_WARM_DAYS / DAYS:.0f} days ({time_since_last_access / DAYS:.2f} days since last access)."
                })
                
        elif current_tier == "Warm":
            # Rule: Warm -> Cold (if not accessed for 60 days)
            if time_since_last_access > DEMOTE_WARM_TO_COLD_DAYS:
                move_plan.append({
                    'id': file_id,
                    'from': 'Warm',
                    'to': 'Cold',
                    'path': current_path,
                    'reason': f"Unused for > {DEMOTE_WARM_TO_COLD_DAYS / DAYS:.0f} days."
                })

        # --- PROMOTION LOGIC (Moving Up) ---
        
        # Note: In our current simulation, files start at 'Hot'.
        # This logic is mainly for files that have already been moved down.

        if current_tier == "Warm" and access_count > PROMOTE_WARM_TO_HOT_COUNT:
            # Rule: Warm -> Hot (if suddenly accessed frequently)
            move_plan.append({
                'id': file_id,
                'from': 'Warm',
                'to': 'Hot',
                'path': current_path,
                'reason': f"Access count is {access_count}, exceeding threshold of {PROMOTE_WARM_TO_HOT_COUNT}."
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

if __name__ == '__main__':
    store = MetadataStore()
    plan = generate_move_plan()
    
    print("\n--- 2. MOVE PLAN GENERATED ---")
    
    if plan:
        print(f"Total Moves Recommended: {len(plan)}\n")
        
        for move in plan:
            # Execute the move for each recommendation
            execute_move(move, store)
            
    else:
        print("No moves are currently recommended based on the tiering rules.")
        
    store.close()
    print("\nTiering Engine execution complete.")
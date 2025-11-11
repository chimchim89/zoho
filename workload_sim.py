import os
import uuid
import time
import random
import csv
from metadata_store import MetadataStore 

# --- Configuration (UPDATE THIS PATH!) ---
# Ensure this matches the correct path you set up in Step 1.2
TIER_PATH = "C:\\Users\\aacha\\OneDrive\\Desktop\\zoho\\mnt_ssd" 
LOG_FILE = "access_log.csv"
NUM_INITIAL_FILES = 8
SIMULATION_DURATION_SECONDS = 20  # Simulate access over 20 seconds for a quick test
ACCESS_EVENTS = 50 # Total number of access events to log

# --- 1. File Creation Function ---

def create_dummy_file(index, store):
    """Creates a small file and registers it as 'Hot' in the database."""
    file_id = f"doc_{index}_{uuid.uuid4().hex[:6]}"
    file_name = f"{file_id}.txt"
    file_path = os.path.join(TIER_PATH, file_name)
    
    # Create simple content
    file_content = f"Data for {file_id}. Size: {random.randint(100, 500)} bytes." * 5 

    try:
        with open(file_path, 'w') as f:
            f.write(file_content)
        
        if store.insert_new_file(file_id, file_path, "Hot"):
            return file_id # Return the ID for use in the log
        
    except Exception as e:
        print(f" [E] Error creating file {file_name}: {e}")
        return None

# --- 2. Log Generation Function ---

def simulate_access(file_ids):
    """Generates random access events and logs them to a CSV file."""
    
    # Create the CSV and write header
    with open(LOG_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['timestamp', 'file_id', 'access_type'])
    
    print(f"\nStarting simulation for {len(file_ids)} files over {SIMULATION_DURATION_SECONDS}s...")
    
    log_data = []
    
    for i in range(ACCESS_EVENTS):
        # Simulate time passing (e.g., small sleep)
        time.sleep(SIMULATION_DURATION_SECONDS / ACCESS_EVENTS) 

        # Pick a file: skew towards the first few files to simulate 'Hot' data
        # Files 0 and 1 are 60% likely to be chosen (Hot)
        # Files 2 through 7 are 40% likely to be chosen (Warm/Cold)
        weights = [0.30, 0.30, 0.05, 0.05, 0.05, 0.05, 0.10, 0.10]
        
        chosen_id = random.choices(file_ids, weights=weights, k=1)[0]
        
        # Log the event
        current_timestamp = time.time()
        log_data.append([current_timestamp, chosen_id, 'READ'])
        
        print(f" [+] Logged access for {chosen_id} (Event {i+1}/{ACCESS_EVENTS})", end='\r')

    # Write all log data to the CSV
    with open(LOG_FILE, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(log_data)
        
    print(f"\nSimulation complete. {len(log_data)} access events logged to {LOG_FILE}.")


if __name__ == '__main__':
    """Main execution block."""
    if not os.path.exists(TIER_PATH):
        print(f"FATAL ERROR: Hot Tier path {TIER_PATH} does not exist. Please check your configuration.")
    else:
        # 1. Initialize the Metadata Store
        store = MetadataStore()
        
        # 2. Create the files and get their IDs
        print("--- 1. CREATING INITIAL DATA FILES ---")
        all_file_ids = []
        for i in range(NUM_INITIAL_FILES):
            file_id = create_dummy_file(i + 1, store)
            if file_id:
                all_file_ids.append(file_id)

        # 3. Run the simulation
        if all_file_ids:
            print("\n--- 2. RUNNING WORKLOAD SIMULATION ---")
            simulate_access(all_file_ids)
        
        # 4. Cleanup
        store.close()
        print("\nWorkload setup complete.")
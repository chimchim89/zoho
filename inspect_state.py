import os
import sqlite3
from pprint import pprint

BASE = os.path.dirname(__file__)
SSD = os.path.join(BASE, 'mnt_ssd')
HDD = os.path.join(BASE, 'mnt_hdd')
CLOUD = os.path.join(BASE, 'mnt_cloud')
DB = os.path.join(BASE, 'tiering_metadata.db')

def list_dir(path):
    try:
        return sorted(os.listdir(path))
    except Exception as e:
        return f'Error reading {path}: {e}'

def read_db(db_path):
    if not os.path.exists(db_path):
        return 'DB not found'
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute('SELECT file_id, current_path, current_tier, access_pattern_score FROM files;')
        rows = cur.fetchall()
    except Exception as e:
        rows = f'Error querying DB: {e}'
    conn.close()
    return rows

if __name__ == '__main__':
    print('--- Files in tiers ---')
    print('SSD count:', len(list_dir(SSD)) if isinstance(list_dir(SSD), list) else list_dir(SSD))
    print('HDD count:', len(list_dir(HDD)) if isinstance(list_dir(HDD), list) else list_dir(HDD))
    print('CLOUD count:', len(list_dir(CLOUD)) if isinstance(list_dir(CLOUD), list) else list_dir(CLOUD))
    print('\nSSD sample:', list_dir(SSD)[:20])
    print('HDD sample:', list_dir(HDD)[:20])
    print('CLOUD sample:', list_dir(CLOUD)[:20])

    print('\n--- DB records ---')
    rows = read_db(DB)
    pprint(rows)
    # Build sets of names per tier to use for verification and collision detection
    ssd = set(list_dir(SSD)) if isinstance(list_dir(SSD), list) else set()
    hdd = set(list_dir(HDD)) if isinstance(list_dir(HDD), list) else set()
    cloud = set(list_dir(CLOUD)) if isinstance(list_dir(CLOUD), list) else set()

    # For each DB record, verify the physical file exists at the recorded path
    print('\n--- DB vs FS verification ---')
    issues = []
    for r in rows:
        try:
            file_id, recorded_path, tier, score = r
        except Exception:
            continue
        exists = os.path.exists(recorded_path)
        basename = os.path.basename(recorded_path)
        found_elsewhere = None
        if not exists:
            # look for basename in SSD/HDD/CLOUD sets
            if basename in ssd:
                found_elsewhere = ('SSD', os.path.join(SSD, basename))
            elif basename in hdd:
                found_elsewhere = ('HDD', os.path.join(HDD, basename))
            elif basename in cloud:
                found_elsewhere = ('CLOUD', os.path.join(CLOUD, basename))
        if not exists and found_elsewhere:
            issues.append((file_id, recorded_path, tier, found_elsewhere))
        elif not exists and not found_elsewhere:
            issues.append((file_id, recorded_path, tier, None))

    print('Records with missing physical file or located elsewhere:')
    pprint(issues)

    # Detect filename collisions across tiers
    print('\n--- Filename collisions ---')
    ssd = set(list_dir(SSD)) if isinstance(list_dir(SSD), list) else set()
    hdd = set(list_dir(HDD)) if isinstance(list_dir(HDD), list) else set()
    cloud = set(list_dir(CLOUD)) if isinstance(list_dir(CLOUD), list) else set()
    all_names = ssd | hdd | cloud
    collisions = []
    for name in all_names:
        locations = []
        if name in ssd:
            locations.append('SSD')
        if name in hdd:
            locations.append('HDD')
        if name in cloud:
            locations.append('CLOUD')
        if len(locations) > 1:
            collisions.append((name, locations))
    pprint(collisions)
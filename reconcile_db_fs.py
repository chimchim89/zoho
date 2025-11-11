import os
import shutil
import sqlite3
from pprint import pprint

BASE = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE, 'tiering_metadata.db')
BACKUP_PATH = os.path.join(BASE, 'tiering_metadata.db.bak')
SSD = os.path.join(BASE, 'mnt_ssd')
HDD = os.path.join(BASE, 'mnt_hdd')
CLOUD = os.path.join(BASE, 'mnt_cloud')

def find_file_basename(basename):
    # Search SSD, HDD, CLOUD for basename; return (tier, fullpath) or (None, None)
    for tier, path in [('Hot', SSD), ('Warm', HDD), ('Cold', CLOUD)]:
        candidate = os.path.join(path, basename)
        if os.path.exists(candidate):
            return tier, candidate
    return None, None

def backup_db():
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f'Backed up DB to {BACKUP_PATH}')
    else:
        print('No DB file found to backup.')

def reconcile():
    if not os.path.exists(DB_PATH):
        print('No DB found at', DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute('SELECT file_id, current_path, current_tier FROM files;')
    rows = cur.fetchall()

    updated = []
    not_found = []

    for file_id, recorded_path, recorded_tier in rows:
        if recorded_path and os.path.exists(recorded_path):
            # Path exists — nothing to do
            continue

        basename = os.path.basename(recorded_path) if recorded_path else None
        if not basename:
            not_found.append((file_id, recorded_path, 'no basename'))
            continue

        tier, actual_path = find_file_basename(basename)
        if tier and actual_path:
            # Update DB row to reflect actual file
            new_tier = 'Hot' if tier == 'Hot' else ('Warm' if tier == 'Warm' else 'Cold')
            cur.execute('UPDATE files SET current_path = ?, current_tier = ? WHERE file_id = ?;', (actual_path, new_tier, file_id))
            updated.append((file_id, recorded_path, actual_path, new_tier))
        else:
            not_found.append((file_id, recorded_path, None))

    conn.commit()
    conn.close()

    print('\nReconciliation summary:')
    print(f' Updated rows: {len(updated)}')
    pprint(updated)
    print(f' Not found rows: {len(not_found)}')
    pprint(not_found)

if __name__ == '__main__':
    print('Starting safe DB <-> FS reconciliation')
    backup_db()
    reconcile()
    print('Reconciliation complete.')
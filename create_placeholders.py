import os
import shutil
import sqlite3
import time

BASE = os.path.dirname(__file__)
DB_PATH = os.path.join(BASE, 'tiering_metadata.db')
BACKUP_PATH = os.path.join(BASE, f'tiering_metadata.db.bak.{int(time.time())}')
SSD = os.path.join(BASE, 'mnt_ssd')
HDD = os.path.join(BASE, 'mnt_hdd')
CLOUD = os.path.join(BASE, 'mnt_cloud')

PLACEHOLDER_CONTENT = 'Placeholder file created to reconcile metadata with filesystem.\n'

def backup_db():
    if os.path.exists(DB_PATH):
        shutil.copy2(DB_PATH, BACKUP_PATH)
        print(f'Backed up DB to {BACKUP_PATH}')
    else:
        print('No DB found to back up.')

def ensure_dirs():
    os.makedirs(SSD, exist_ok=True)
    os.makedirs(HDD, exist_ok=True)
    os.makedirs(CLOUD, exist_ok=True)

def create_placeholder(basename, target_dir):
    path = os.path.join(target_dir, basename)
    if os.path.exists(path):
        return path
    with open(path, 'w') as f:
        f.write(PLACEHOLDER_CONTENT)
    return path

def recreate_missing(use_target='ssd'):
    if not os.path.exists(DB_PATH):
        print('DB not found:', DB_PATH)
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute('SELECT file_id, current_path, current_tier FROM files;')
    rows = cur.fetchall()

    created = []
    skipped = []

    target_map = {'ssd': SSD, 'hdd': HDD, 'cloud': CLOUD}
    target_dir = target_map.get(use_target, SSD)

    for file_id, recorded_path, recorded_tier in rows:
        if recorded_path and os.path.exists(recorded_path):
            continue

        # determine basename to create
        if recorded_path and os.path.basename(recorded_path):
            basename = os.path.basename(recorded_path)
        else:
            # create a reasonable filename from file_id
            basename = f'{file_id}.txt'

        created_path = create_placeholder(basename, target_dir)
        # update DB to point to created path and mark as Hot
        cur.execute('UPDATE files SET current_path = ?, current_tier = ? WHERE file_id = ?;', (created_path, 'Hot', file_id))
        created.append((file_id, created_path))

    conn.commit()
    conn.close()

    print('Placeholders created for', len(created), 'entries')
    for c in created:
        print('  ', c)

if __name__ == '__main__':
    print('Creating placeholders to reconcile missing DB entries...')
    backup_db()
    ensure_dirs()
    recreate_missing(use_target='ssd')
    print('Done.')
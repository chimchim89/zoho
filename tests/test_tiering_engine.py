import sys
import os
import time

# Ensure project root is on sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from metadata_store import MetadataStore
import tiering_engine as te


def make_ts_days_ago(days):
    return time.time() - days * 24 * 3600


def test_generate_move_plan_respects_pattern_and_time():
    store = MetadataStore(':memory:')

    # File A: Hot, old last access, low pattern -> should be demoted to Warm
    store.insert_new_file('fileA', '/mnt_ssd/fileA', current_tier='Hot')
    store.update_file_stats('fileA', make_ts_days_ago(30), 0, 0.0)

    # File B: Hot, recent access, high pattern -> should stay Hot
    store.insert_new_file('fileB', '/mnt_ssd/fileB', current_tier='Hot')
    store.update_file_stats('fileB', make_ts_days_ago(1), 15, 0.9)

    # File C: Warm, moderate access but high pattern -> should be promoted to Hot
    store.insert_new_file('fileC', '/mnt_hdd/fileC', current_tier='Warm')
    store.update_file_stats('fileC', make_ts_days_ago(5), 5, 0.8)

    plan = te.generate_move_plan(store=store)

    # Build a dict for quick lookup
    moves = {m['id']: m for m in plan}

    assert 'fileA' in moves and moves['fileA']['from'] == 'Hot' and moves['fileA']['to'] == 'Warm'
    assert 'fileB' not in moves or moves['fileB']['to'] == 'Hot'
    assert 'fileC' in moves and moves['fileC']['from'] == 'Warm' and moves['fileC']['to'] == 'Hot'

    store.close()

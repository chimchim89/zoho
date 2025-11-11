import sys
import os
import time

# Ensure project root is on sys.path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analyzer import compute_ewma


def test_compute_ewma_initial():
    # If previous_score is None, should return new_sample
    assert compute_ewma(None, 0.5, alpha=0.3) == 0.5


def test_compute_ewma_update():
    prev = 0.2
    new = 0.8
    alpha = 0.5
    expected = alpha * new + (1 - alpha) * prev
    assert compute_ewma(prev, new, alpha=alpha) == expected

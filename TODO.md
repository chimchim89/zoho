# Tiering Engine - Future Improvements (TODO)

## 1. Add Cost and Capacity Awareness

Right now, the engine moves files without considering the consequences. A truly smart system would be aware of its environment.

- **Tier Capacity:** What if the "Hot" tier is 95% full? The engine should become more aggressive about demoting files. We could dynamically lower the `DEMOTE_HOT_TO_WARM_DAYS` threshold from 14 to 7, or lower the `PATTERN_PROTECT_THRESHOLD` to be more selective about what stays.
- **Cost Analysis:** Moving data, especially out of the cloud, has a cost. The engine could have a "cost budget" and prioritize moves that give the most "bang for the buck" (e.g., moving a few very large, very cold files might be more cost-effective than moving many small ones).

## 2. Deeper Pattern and Content Analysis

We can extract much more information than just access times.

- **File Type Heuristics:** We could add logic to treat file types differently. For example, log files (`.log`, `.gz`) are often written once and rarely read again, making them prime candidates for immediate demotion to a cheaper tier after a short grace period. In contrast, database files or virtual machine images might be protected.
- **Predictive Prefetching:** If we analyze the access logs more deeply, we might find that `file_A` is almost always accessed within minutes of `file_B`. If the engine sees a request for `file_B` (which is in Cold storage), it could intelligently prefetch `file_A` into the Warm tier at the same time, anticipating the user's next request and reducing latency.
# zoho — Storage Tiering Simulator

This repository contains a small storage-tiering simulation and engine that moves files between Hot (SSD), Warm (HDD), and Cold (cloud) tiers. It includes a lightweight metadata store (SQLite), a workload simulator, an analyzer that computes access-pattern scores, and a tiering engine that generates and executes move plans.

## Key files

- `workload_sim.py` — create sample files in the Hot tier and generate an `access_log.csv` of read events.
- `analyzer.py` — reads `access_log.csv`, computes an EWMA-based access pattern score per file, and updates `tiering_metadata.db`.
- `tiering_engine.py` — reads metadata, applies tiering rules (time + pattern score), generates a move plan, and executes moves. Supports a local simulated cloud (`mnt_cloud/`) or real S3.
- `metadata_store.py` — wraps the SQLite DB and includes a small migration to add `access_pattern_score` if missing.
- `config.json` — project configuration (thresholds, local-cloud settings). See section below.

## Quick start (Windows PowerShell)

1. Ensure the project directories exist (the repo already includes `mnt_ssd`, `mnt_hdd`, `mnt_cloud`):

```powershell
cd 'C:\Users\aacha\OneDrive\Desktop\zoho'
ls
```

2. Create initial files and a workload log (this will write files into the Hot tier and generate `access_log.csv`):

```powershell
python workload_sim.py
```

3. Run the analyzer to compute and store pattern scores (EWMA alpha is configurable):

```powershell
python analyzer.py --alpha 0.3
```

4. Dry-run the tiering engine to see move recommendations and pattern scores (no physical moves):

```powershell
python tiering_engine.py --dry-run --show-scores --use-local-cloud true
```

5. Execute the move plan (will perform local moves or S3 uploads depending on config):

```powershell
python tiering_engine.py --use-local-cloud true
```

## Configuration (`config.json`)

The repository includes a `config.json` with sensible defaults. Key fields:

- `demote_hot_to_warm_days` — days of inactivity before Hot->Warm demotion (default: 14)
- `demote_warm_to_cold_days` — days of inactivity before Warm->Cold demotion (default: 60)
- `promote_cold_to_warm_days` — days used for Cold->Warm promotion rule (default: 1; fine for tests)
- `promote_warm_to_hot_count` — numeric access count threshold to promote Warm->Hot (default: 10)
- `pattern_protect_threshold` — pattern score above which Hot files are protected from demotion (0-1)
- `warm_to_cold_pattern_block` — pattern score above which Warm->Cold is blocked
- `promote_pattern_threshold` — pattern score at which Warm files are promoted to Hot
- `use_local_cloud` — `true` to store Cold-tier files under `mnt_cloud/` (default: true)
- `local_cloud_path` — path to local cloud directory (default: `mnt_cloud/`)

Edit `config.json` to tune thresholds without modifying code.

## How pattern scores work

The analyzer computes a sample value per file based on recency and frequency (combination), and then updates the stored `access_pattern_score` using an EWMA (alpha configurable with `--alpha`). This helps the tiering engine recognize trending/bursty files even if last-access timestamps are old.

## Local-cloud vs S3

- Local-cloud mode (`use_local_cloud: true`) moves Cold-tier files to `mnt_cloud/` for easy testing.
- To use AWS S3 instead, set `use_local_cloud` to `false`, configure `S3_BUCKET_NAME` and `AWS_REGION` in `tiering_engine.py`, and ensure AWS credentials are available (e.g., via environment / AWS CLI).

## Recommended next steps

- Add unit tests for `analyzer.py` and `generate_move_plan()` in `tiering_engine.py`.
- Add logging (instead of prints) and a small CLI config parser to override `config.json` at runtime.
- If you plan to use S3, add retry/backoff and verify credentials.

If you'd like, I can add a `README` section with example outputs, or add unit tests next. Which would you like me to do? (I can add tests or documentation examples.)

#!/usr/bin/env python3
"""
Batch runner for resource-friendly pipeline.
Processes all transaction files in parallel.
"""
import os
import glob
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed

OWNERS_FILE = "data/raw/owners/20250716/Dubai Hills.csv"
TRANSACTIONS_DIR = "data/raw/transactions/20250716/"
OUTPUT_DIR = "data/processed/resource_friendly/"
MAX_PARALLEL = 7  # Resource-friendly: use 7 cores

# Find all transaction files
transaction_files = sorted(glob.glob(os.path.join(TRANSACTIONS_DIR, "corrected_transactions_part_*.csv")))

os.makedirs(OUTPUT_DIR, exist_ok=True)

jobs = []
with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as executor:
    futures = []
    for txn_file in transaction_files:
        part = os.path.splitext(os.path.basename(txn_file))[0]
        run_id = f"resource_friendly_{part}"
        cmd = [
            "python", "run_resource_friendly.py",
            "--owners", OWNERS_FILE,
            "--transactions", txn_file,
            "--output", OUTPUT_DIR,
            "--run-id", run_id
        ]
        log_file = os.path.join(OUTPUT_DIR, f"{run_id}.log")
        print(f"Starting: {txn_file} -> {log_file}")
        futures.append(executor.submit(
            subprocess.run, cmd, stdout=open(log_file, "w"), stderr=subprocess.STDOUT
        ))
    # Wait for all jobs to finish
    for future in as_completed(futures):
        print("One job finished.")

print("All transaction files processed!") 
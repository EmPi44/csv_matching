#!/usr/bin/env python3
"""
Monitor script to check when ultra-fast pipeline results appear.
"""

import os
import time
from pathlib import Path

def check_results():
    """Check for results in the ultra-fast pipeline output directory."""
    base_dir = "data/processed/ultra_fast_pipeline"
    
    if not os.path.exists(base_dir):
        print("❌ Output directory doesn't exist yet")
        return
    
    # Check for individual chunk results
    chunk_dirs = [d for d in os.listdir(base_dir) if d.startswith("ultra_chunk_")]
    
    print(f"📁 Found {len(chunk_dirs)} chunk directories")
    
    total_files = 0
    for chunk_dir in chunk_dirs:
        chunk_path = os.path.join(base_dir, chunk_dir)
        files = [f for f in os.listdir(chunk_path) if f.endswith(('.parquet', '.csv'))]
        if files:
            print(f"✅ {chunk_dir}: {len(files)} files")
            total_files += len(files)
        else:
            print(f"⏳ {chunk_dir}: No files yet")
    
    # Check for combined results
    combined_files = []
    for file in os.listdir(base_dir):
        if file.startswith("ultra_combined"):
            combined_files.append(file)
    
    if combined_files:
        print(f"\n🎉 COMBINED RESULTS READY:")
        for file in combined_files:
            print(f"   📄 {file}")
    else:
        print(f"\n⏳ Combined results not ready yet")
    
    print(f"\n📊 Total files found: {total_files}")
    return total_files

def main():
    """Main monitoring loop."""
    print("🔍 Monitoring ultra-fast pipeline results...")
    print("Press Ctrl+C to stop\n")
    
    try:
        while True:
            print(f"\n{'='*50}")
            print(f"🕐 {time.strftime('%H:%M:%S')}")
            print('='*50)
            
            files_found = check_results()
            
            if files_found > 0:
                print(f"\n🎯 RESULTS FOUND! Check the directories above.")
            
            print(f"\n⏳ Checking again in 30 seconds...")
            time.sleep(30)
            
    except KeyboardInterrupt:
        print(f"\n👋 Monitoring stopped")

if __name__ == "__main__":
    main() 
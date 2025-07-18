#!/usr/bin/env python3
"""
Check for partial results during ultra-fast pipeline processing.
"""

import os
import pandas as pd
from pathlib import Path

def check_partial_results():
    """Check for partial results in the ultra-fast pipeline output directory."""
    base_dir = "data/processed/ultra_fast_pipeline"
    
    if not os.path.exists(base_dir):
        print("❌ Output directory doesn't exist yet")
        return
    
    print("🔍 Checking for partial results...\n")
    
    # Check for individual chunk results
    chunk_dirs = [d for d in os.listdir(base_dir) if d.startswith("ultra_chunk_")]
    chunk_dirs.sort()
    
    completed_chunks = 0
    total_matches = 0
    
    for chunk_dir in chunk_dirs:
        chunk_path = os.path.join(base_dir, chunk_dir)
        matches_file = os.path.join(chunk_path, "ultra_fast_matches.parquet")
        
        if os.path.exists(matches_file):
            try:
                df = pd.read_parquet(matches_file)
                chunk_matches = len(df)
                total_matches += chunk_matches
                completed_chunks += 1
                
                print(f"✅ {chunk_dir}: {chunk_matches:,} matches")
                
                # Show sample matches
                if chunk_matches > 0:
                    print(f"   📊 Sample matches:")
                    sample = df.head(3)
                    for _, row in sample.iterrows():
                        print(f"      {row['owner_id']} → {row['txn_id']} ({row['match_type']}, {row['confidence']:.1%})")
                    print()
                
            except Exception as e:
                print(f"❌ {chunk_dir}: Error reading file - {e}")
        else:
            print(f"⏳ {chunk_dir}: Still processing...")
    
    # Check for combined results
    combined_file = os.path.join(base_dir, "ultra_combined_matches.parquet")
    if os.path.exists(combined_file):
        try:
            df = pd.read_parquet(combined_file)
            print(f"\n🎉 COMBINED RESULTS READY!")
            print(f"📊 Total matches: {len(df):,}")
            print(f"📁 File: {combined_file}")
            
            # Show summary statistics
            tier1_count = len(df[df['match_type'] == 'deterministic'])
            tier2_count = len(df[df['match_type'] == 'fuzzy'])
            
            print(f"\n📈 Match Breakdown:")
            print(f"   Tier 1 (Exact): {tier1_count:,}")
            print(f"   Tier 2 (Fuzzy): {tier2_count:,}")
            
            if tier2_count > 0:
                avg_confidence = df[df['match_type'] == 'fuzzy']['confidence'].mean()
                print(f"   Avg Fuzzy Confidence: {avg_confidence:.1%}")
            
        except Exception as e:
            print(f"❌ Error reading combined file: {e}")
    
    # Summary
    print(f"\n📊 Progress Summary:")
    print(f"   Completed chunks: {completed_chunks}/{len(chunk_dirs)}")
    print(f"   Total matches so far: {total_matches:,}")
    
    if completed_chunks > 0:
        avg_per_chunk = total_matches / completed_chunks
        estimated_total = avg_per_chunk * len(chunk_dirs)
        print(f"   Average per chunk: {avg_per_chunk:,.0f}")
        print(f"   Estimated total: {estimated_total:,.0f}")
    
    return completed_chunks, total_matches

def show_sample_data():
    """Show sample data from the first completed chunk."""
    base_dir = "data/processed/ultra_fast_pipeline"
    
    # Find first completed chunk
    chunk_dirs = [d for d in os.listdir(base_dir) if d.startswith("ultra_chunk_")]
    chunk_dirs.sort()
    
    for chunk_dir in chunk_dirs:
        chunk_path = os.path.join(base_dir, chunk_dir)
        matches_file = os.path.join(chunk_path, "ultra_fast_matches.parquet")
        
        if os.path.exists(matches_file):
            print(f"\n📄 Sample data from {chunk_dir}:")
            print("=" * 60)
            
            try:
                df = pd.read_parquet(matches_file)
                print(f"Total matches: {len(df):,}")
                print(f"Columns: {list(df.columns)}")
                print("\nFirst 5 matches:")
                print(df.head().to_string(index=False))
                
                # Show statistics
                print(f"\n📈 Statistics:")
                print(f"   Match types: {df['match_type'].value_counts().to_dict()}")
                if 'confidence' in df.columns:
                    print(f"   Confidence range: {df['confidence'].min():.1%} - {df['confidence'].max():.1%}")
                    print(f"   Average confidence: {df['confidence'].mean():.1%}")
                
                break
                
            except Exception as e:
                print(f"Error reading file: {e}")
                break

def main():
    """Main function."""
    print("🔍 Ultra-Fast Pipeline Partial Results Checker")
    print("=" * 50)
    
    completed, total = check_partial_results()
    
    if completed > 0:
        print(f"\n💡 Want to see sample data? Run:")
        print(f"   python check_partial_results.py --sample")
        
        # Check if sample flag is provided
        import sys
        if len(sys.argv) > 1 and sys.argv[1] == '--sample':
            show_sample_data()
    
    print(f"\n⏰ Check again in a few minutes for more results!")

if __name__ == "__main__":
    main() 
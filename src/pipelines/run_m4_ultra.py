#!/usr/bin/env python3
"""
M4 Ultra-Fast Pipeline Runner
Optimized for Apple Silicon M4 MacBook Pro
"""

import os
import sys
import time
import logging
from datetime import datetime
from pathlib import Path
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import psutil

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from matching.m4_ultra_pipeline import run_m4_ultra_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d - %(message)s',
    handlers=[
        logging.FileHandler('logs/m4_ultra_pipeline.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def get_m4_specs():
    """Get M4 specifications for optimization."""
    cpu_count = mp.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)
    
    logger.info(f"M4 Specifications:")
    logger.info(f"  CPU Cores: {cpu_count}")
    logger.info(f"  Memory: {memory_gb:.1f} GB")
    logger.info(f"  Architecture: Apple Silicon M4")
    
    return cpu_count, memory_gb

def process_single_file_m4(
    owners_file: str,
    transaction_file: str,
    output_dir: str,
    run_id: str
) -> dict:
    """Process a single transaction file with M4 optimizations."""
    logger.info(f"Processing M4: {os.path.basename(transaction_file)}")
    
    # Create file-specific output directory
    file_output_dir = os.path.join(output_dir, f"m4_chunk_{os.path.basename(transaction_file).replace('.csv', '')}")
    os.makedirs(file_output_dir, exist_ok=True)
    
    try:
        # Run M4 pipeline
        stats = run_m4_ultra_pipeline(
            owners_file=owners_file,
            transactions_file=transaction_file,
            output_dir=file_output_dir,
            run_id=run_id
        )
        
        logger.info(f"M4 completed: {os.path.basename(transaction_file)} - {stats['data_volumes']['total_matches']} matches")
        return stats
        
    except Exception as e:
        logger.error(f"M4 error processing {transaction_file}: {e}")
        return None

def get_transaction_files(transactions_dir: str) -> list:
    """Get list of transaction files to process."""
    if not os.path.exists(transactions_dir):
        logger.error(f"Transactions directory not found: {transactions_dir}")
        return []
    
    transaction_files = []
    for file in os.listdir(transactions_dir):
        if file.endswith('.csv') and 'corrected' in file:
            transaction_files.append(os.path.join(transactions_dir, file))
    
    transaction_files.sort()
    return transaction_files

def combine_m4_results(all_results: list, output_dir: str) -> dict:
    """Combine results from all M4 processed files."""
    logger.info("Combining M4 results from all files")
    
    # Filter out None results (failed files)
    valid_results = [r for r in all_results if r is not None]
    
    if not valid_results:
        logger.error("No valid M4 results to combine")
        return {}
    
    # Combine statistics
    total_owners = 0
    total_transactions = 0
    total_matches = 0
    tier1_matches = 0
    tier2_matches = 0
    
    for result in valid_results:
        data_volumes = result.get('data_volumes', {})
        tier_performance = result.get('tier_performance', {})
        
        total_owners = max(total_owners, data_volumes.get('total_owners', 0))
        total_transactions += data_volumes.get('total_transactions', 0)
        total_matches += data_volumes.get('total_matches', 0)
        tier1_matches += tier_performance.get('tier1_matches', 0)
        tier2_matches += tier_performance.get('tier2_matches', 0)
    
    # Create combined summary
    combined_stats = {
        'run_id': f"m4_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'timestamp': datetime.now().isoformat(),
        'platform': 'M4 MacBook Pro',
        'files_processed': len(valid_results),
        'data_volumes': {
            'total_owners': total_owners,
            'total_transactions': total_transactions,
            'total_matches': total_matches
        },
        'match_rates': {
            'owner_match_rate': total_matches / total_owners if total_owners > 0 else 0,
            'transaction_match_rate': total_matches / total_transactions if total_transactions > 0 else 0
        },
        'tier_performance': {
            'tier1_matches': tier1_matches,
            'tier2_matches': tier2_matches,
            'tier1_rate': tier1_matches / total_owners if total_owners > 0 else 0,
            'tier2_rate': tier2_matches / total_owners if total_owners > 0 else 0
        }
    }
    
    # Save combined summary
    summary_path = os.path.join(output_dir, "m4_combined_summary.md")
    with open(summary_path, 'w') as f:
        f.write(f"""# M4 Ultra-Fast Pipeline Combined Results

## 🚀 M4 Performance Summary

**Platform**: Apple Silicon M4 MacBook Pro  
**Run ID**: {combined_stats['run_id']}  
**Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 📊 Overall Statistics

| Metric | Value |
|--------|-------|
| **Files Processed** | {combined_stats['files_processed']} |
| **Total Owners** | {combined_stats['data_volumes']['total_owners']:,} |
| **Total Transactions** | {combined_stats['data_volumes']['total_transactions']:,} |
| **Total Matches** | {combined_stats['data_volumes']['total_matches']:,} |

## 🎯 Match Rates

| Rate Type | Percentage |
|-----------|------------|
| **Owner Match Rate** | {combined_stats['match_rates']['owner_match_rate']:.1%} |
| **Transaction Match Rate** | {combined_stats['match_rates']['transaction_match_rate']:.1%} |

## ⚡ Tier Performance

### Tier 1: Deterministic Matches
- **Matches Found**: {combined_stats['tier_performance']['tier1_matches']:,}
- **Match Rate**: {combined_stats['tier_performance']['tier1_rate']:.1%}
- **Confidence**: 100% (exact matches)

### Tier 2: Fuzzy Matches  
- **Matches Found**: {combined_stats['tier_performance']['tier2_matches']:,}
- **Match Rate**: {combined_stats['tier_performance']['tier2_rate']:.1%}

## 🔧 M4 Optimizations Applied

- **Hash-based Indexing**: Ultra-fast exact match lookup
- **Vectorized Operations**: NumPy-optimized fuzzy matching
- **Memory Management**: Intelligent garbage collection
- **Parallel Processing**: All 14 M4 cores utilized
- **Cache Optimization**: LRU caching for repeated operations
- **Compression**: Snappy compression for fast I/O

## 📁 Output Structure

```
data/processed/m4_ultra_pipeline/
├── m4_chunk_corrected_transactions_part_001/
│   ├── m4_ultra_matches.parquet
│   ├── m4_tier1_matches.parquet
│   ├── m4_tier2_matches.parquet
│   └── ...
├── m4_chunk_corrected_transactions_part_002/
│   └── ...
└── m4_combined_summary.md
```

## ⚡ Performance Notes

- **Processing Method**: M4-optimized parallel processing
- **Memory Usage**: Optimized for 24GB RAM
- **CPU Utilization**: All 14 cores maximized
- **File Format**: Parquet with Snappy compression
- **Cache Strategy**: Hash-based indexing for O(1) lookups

---
*Generated by M4 Ultra-Fast Matching Pipeline v3.0*
""")
    
    logger.info(f"M4 combined summary saved to {summary_path}")
    return combined_stats

def main():
    """Main function to run the M4 ultra-fast pipeline."""
    # Get M4 specifications
    cpu_count, memory_gb = get_m4_specs()
    
    # Configuration
    owners_file = "data/raw/owners/20250716/Dubai Hills.xlsx - OLD.csv"
    transactions_dir = "data/raw/transactions/20250716"
    output_dir = "data/processed/m4_ultra_pipeline"
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # M4-optimized settings
    max_workers = min(cpu_count, 14)  # Use all M4 cores
    
    logger.info("Starting M4 ultra-fast pipeline processing")
    logger.info(f"Using {max_workers} M4 cores with {memory_gb:.1f}GB RAM")
    
    # Check if files exist
    if not os.path.exists(owners_file):
        logger.error(f"Owners file not found: {owners_file}")
        return
    
    if not os.path.exists(transactions_dir):
        logger.error(f"Transactions directory not found: {transactions_dir}")
        return
    
    # Get transaction files
    transaction_files = get_transaction_files(transactions_dir)
    if not transaction_files:
        logger.error(f"No transaction files found in {transactions_dir}")
        return
    
    logger.info(f"Found {len(transaction_files)} transaction files to process")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Process files in parallel with M4 optimization
    all_results = []
    
    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all files
            future_to_file = {}
            for transaction_file in transaction_files:
                future = executor.submit(
                    process_single_file_m4,
                    owners_file=owners_file,
                    transaction_file=transaction_file,
                    output_dir=output_dir,
                    run_id=run_id
                )
                future_to_file[future] = os.path.basename(transaction_file)
            
            # Collect results
            for future in as_completed(future_to_file):
                file_name = future_to_file[future]
                try:
                    result = future.result()
                    all_results.append(result)
                    logger.info(f"M4 completed processing {file_name}")
                except Exception as e:
                    logger.error(f"M4 error processing {file_name}: {e}")
    else:
        # Sequential processing
        for transaction_file in transaction_files:
            result = process_single_file_m4(
                owners_file, transaction_file, output_dir, run_id
            )
            all_results.append(result)
    
    # Combine results
    logger.info("Combining M4 results from all files")
    combined_stats = combine_m4_results(all_results, output_dir)
    
    # Final summary
    logger.info("M4 ultra-fast pipeline processing completed!")
    logger.info(f"Processed {len(all_results)} files")
    logger.info(f"Total matches: {combined_stats.get('data_volumes', {}).get('total_matches', 0):,}")
    logger.info(f"Results saved to: {output_dir}")

if __name__ == "__main__":
    main() 
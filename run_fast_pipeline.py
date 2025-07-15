#!/usr/bin/env python3
"""
Fast pipeline runner for Dubai Hills property matching.

Processes multiple transaction files in parallel for maximum speed.
"""

import pandas as pd
import os
import glob
from datetime import datetime
from loguru import logger
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import partial
from matching.fast_pipeline import run_fast_pipeline


def process_transaction_file_fast(
    owners_file: str,
    transaction_file: str,
    output_dir: str,
    run_id: str
) -> dict:
    """
    Process a single transaction file using the fast pipeline.
    
    Args:
        owners_file: Path to owners file
        transaction_file: Path to single transaction file
        output_dir: Output directory
        run_id: Pipeline run ID
        
    Returns:
        Pipeline results for this file
    """
    file_name = os.path.basename(transaction_file)
    logger.info(f"Processing transaction file: {file_name}")
    
    # Create subdirectory for this file's results
    file_output_dir = os.path.join(output_dir, f"fast_part_{file_name.replace('.csv', '')}")
    os.makedirs(file_output_dir, exist_ok=True)
    
    try:
        results = run_fast_pipeline(
            owners_file=owners_file,
            transactions_file=transaction_file,
            output_dir=file_output_dir,
            run_id=f"{run_id}_{file_name.replace('.csv', '')}"
        )
        
        # Add file info to results
        results['file_name'] = file_name
        results['file_output_dir'] = file_output_dir
        
        logger.info(f"Completed processing {file_name}: {results['data_volumes']['total_matches']} matches")
        return results
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")
        return None


def get_transaction_files(transactions_dir: str) -> list:
    """
    Get all transaction CSV files in the directory.
    
    Args:
        transactions_dir: Directory containing transaction files
        
    Returns:
        List of transaction file paths
    """
    pattern = os.path.join(transactions_dir, "*.csv")
    files = glob.glob(pattern)
    files.sort()  # Ensure consistent ordering
    return files


def combine_fast_results(all_results: list, output_dir: str) -> dict:
    """
    Combine results from all transaction files.
    
    Args:
        all_results: List of results from each file
        output_dir: Output directory for combined results
        
    Returns:
        Combined statistics
    """
    logger.info("Combining results from all transaction files")
    
    # Filter out None results (failed files)
    valid_results = [r for r in all_results if r is not None]
    
    if not valid_results:
        logger.error("No valid results to combine")
        return {}
    
    # Combine matches from all files
    all_matches = []
    total_owners = 0
    total_transactions = 0
    total_matches = 0
    tier1_matches = 0
    tier2_matches = 0
    
    for result in valid_results:
        # Load matches from each file
        file_output_dir = result.get('file_output_dir', '')
        matches_file = os.path.join(file_output_dir, 'fast_matches.parquet')
        
        if os.path.exists(matches_file):
            matches_df = pd.read_parquet(matches_file)
            all_matches.append(matches_df)
        
        # Aggregate statistics
        data_volumes = result.get('data_volumes', {})
        tier_performance = result.get('tier_performance', {})
        
        total_owners = max(total_owners, data_volumes.get('total_owners', 0))
        total_transactions += data_volumes.get('total_transactions', 0)
        total_matches += data_volumes.get('total_matches', 0)
        tier1_matches += tier_performance.get('tier1_matches', 0)
        tier2_matches += tier_performance.get('tier2_matches', 0)
    
    # Combine all matches
    if all_matches:
        combined_matches = pd.concat(all_matches, ignore_index=True)
        combined_matches_path = os.path.join(output_dir, "fast_combined_matches.parquet")
        combined_matches.to_parquet(combined_matches_path, index=False)
        logger.info(f"Combined {len(combined_matches)} total matches saved to {combined_matches_path}")
    
    # Create combined summary
    combined_stats = {
        'run_id': f"fast_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'timestamp': datetime.now().isoformat(),
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
    summary_path = os.path.join(output_dir, "fast_combined_summary.md")
    with open(summary_path, 'w') as f:
        f.write(f"""# Fast Pipeline Combined Results

## Overview
- **Files Processed**: {combined_stats['files_processed']}
- **Total Owners**: {combined_stats['data_volumes']['total_owners']:,}
- **Total Transactions**: {combined_stats['data_volumes']['total_transactions']:,}
- **Total Matches**: {combined_stats['data_volumes']['total_matches']:,}

## Match Rates
- **Owner Match Rate**: {combined_stats['match_rates']['owner_match_rate']:.1%}
- **Transaction Match Rate**: {combined_stats['match_rates']['transaction_match_rate']:.1%}

## Tier Performance
- **Tier 1 Matches**: {combined_stats['tier_performance']['tier1_matches']:,}
- **Tier 2 Matches**: {combined_stats['tier_performance']['tier2_matches']:,}
- **Tier 1 Rate**: {combined_stats['tier_performance']['tier1_rate']:.1%}
- **Tier 2 Rate**: {combined_stats['tier_performance']['tier2_rate']:.1%}

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
    
    logger.info(f"Fast combined summary saved to {summary_path}")
    return combined_stats


def main():
    """Main function to run the fast pipeline."""
    # Configure logging
    logger.add("logs/fast_pipeline.log", rotation="1 day", retention="7 days")
    
    # Configuration
    owners_file = "data/raw/owners/20250716/Dubai Hills.xlsx - OLD.csv"
    transactions_dir = "data/raw/transactions/20250716"
    output_dir = "data/processed/fast_pipeline"
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info("Starting fast pipeline processing")
    
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
    
    # Determine number of parallel workers
    max_workers = min(mp.cpu_count(), len(transaction_files), 8)  # Cap at 8 workers
    logger.info(f"Using {max_workers} parallel workers")
    
    # Process files in parallel
    all_results = []
    
    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all files
            future_to_file = {}
            for transaction_file in transaction_files:
                future = executor.submit(
                    process_transaction_file_fast,
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
                    logger.info(f"Completed processing {file_name}")
                except Exception as e:
                    logger.error(f"Error processing {file_name}: {e}")
    else:
        # Sequential processing
        for transaction_file in transaction_files:
            result = process_transaction_file_fast(
                owners_file, transaction_file, output_dir, run_id
            )
            all_results.append(result)
    
    # Combine results
    logger.info("Combining results from all files")
    combined_stats = combine_fast_results(all_results, output_dir)
    
    # Final summary
    logger.info("Fast pipeline processing completed!")
    logger.info(f"Processed {len(all_results)} files")
    logger.info(f"Total matches: {combined_stats.get('data_volumes', {}).get('total_matches', 0):,}")
    logger.info(f"Results saved to: {output_dir}")


if __name__ == "__main__":
    main() 
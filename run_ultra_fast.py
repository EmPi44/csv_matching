#!/usr/bin/env python3
"""
Ultra-fast pipeline runner for Dubai Hills property matching.

Splits files into smaller chunks and uses maximum parallelism for sub-5-minute processing.
"""

import pandas as pd
import os
import glob
from datetime import datetime
from loguru import logger
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from matching.ultra_fast_pipeline import run_ultra_fast_pipeline


def split_large_file(file_path: str, output_dir: str, chunk_size: int = 50000) -> list:
    """
    Split a large CSV file into smaller chunks for parallel processing.
    
    Args:
        file_path: Path to the large file
        output_dir: Directory to save chunks
        chunk_size: Number of rows per chunk
        
    Returns:
        List of chunk file paths
    """
    logger.info(f"Splitting large file: {os.path.basename(file_path)}")
    
    # Read the file in chunks
    chunk_files = []
    chunk_num = 1
    
    for chunk_df in pd.read_csv(file_path, chunksize=chunk_size, low_memory=False):
        chunk_file = os.path.join(output_dir, f"chunk_{chunk_num:03d}.csv")
        chunk_df.to_csv(chunk_file, index=False)
        chunk_files.append(chunk_file)
        chunk_num += 1
        
        logger.info(f"Created chunk {chunk_num-1}: {len(chunk_df)} rows")
    
    logger.info(f"Split file into {len(chunk_files)} chunks")
    return chunk_files


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


def process_chunk_file(
    owners_file: str,
    chunk_file: str,
    output_dir: str,
    run_id: str,
    chunk_size: int = 500
) -> dict:
    """
    Process a single chunk file using the ultra-fast pipeline.
    
    Args:
        owners_file: Path to owners file
        chunk_file: Path to chunk file
        output_dir: Output directory
        run_id: Pipeline run ID
        chunk_size: Size of owner chunks for fuzzy matching
        
    Returns:
        Pipeline results for this chunk
    """
    file_name = os.path.basename(chunk_file)
    logger.info(f"Processing chunk file: {file_name}")
    
    # Create subdirectory for this chunk's results
    chunk_output_dir = os.path.join(output_dir, f"ultra_chunk_{file_name.replace('.csv', '')}")
    os.makedirs(chunk_output_dir, exist_ok=True)
    
    try:
        results = run_ultra_fast_pipeline(
            owners_file=owners_file,
            transactions_file=chunk_file,
            output_dir=chunk_output_dir,
            run_id=f"{run_id}_{file_name.replace('.csv', '')}",
            chunk_size=chunk_size
        )
        
        # Add file info to results
        results['file_name'] = file_name
        results['chunk_output_dir'] = chunk_output_dir
        
        logger.info(f"Completed processing {file_name}: {results['data_volumes']['total_matches']} matches")
        return results
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")
        return None


def combine_ultra_results(all_results: list, output_dir: str) -> dict:
    """
    Combine results from all chunk files.
    
    Args:
        all_results: List of results from each chunk
        output_dir: Output directory for combined results
        
    Returns:
        Combined statistics
    """
    logger.info("Combining results from all chunk files")
    
    # Filter out None results (failed chunks)
    valid_results = [r for r in all_results if r is not None]
    
    if not valid_results:
        logger.error("No valid results to combine")
        return {}
    
    # Combine matches from all chunks
    all_matches = []
    total_owners = 0
    total_transactions = 0
    total_matches = 0
    tier1_matches = 0
    tier2_matches = 0
    
    for result in valid_results:
        # Load matches from each chunk
        chunk_output_dir = result.get('chunk_output_dir', '')
        matches_file = os.path.join(chunk_output_dir, 'ultra_fast_matches.parquet')
        
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
        combined_matches_path = os.path.join(output_dir, "ultra_combined_matches.parquet")
        combined_matches.to_parquet(combined_matches_path, index=False)
        logger.info(f"Combined {len(combined_matches)} total matches saved to {combined_matches_path}")
    
    # Create combined summary
    combined_stats = {
        'run_id': f"ultra_combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        'timestamp': datetime.now().isoformat(),
        'chunks_processed': len(valid_results),
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
    summary_path = os.path.join(output_dir, "ultra_combined_summary.md")
    with open(summary_path, 'w') as f:
        f.write(f"""# Ultra-Fast Pipeline Combined Results

## Overview
- **Chunks Processed**: {combined_stats['chunks_processed']}
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
    
    logger.info(f"Ultra combined summary saved to {summary_path}")
    return combined_stats


def main():
    """Main function to run the ultra-fast pipeline."""
    # Configure logging
    logger.add("logs/ultra_fast_pipeline.log", rotation="1 day", retention="7 days")
    
    # Configuration
    owners_file = "data/raw/owners/20250716/Dubai Hills.xlsx - OLD.csv"
    transactions_dir = "data/raw/transactions/20250716"
    output_dir = "data/processed/ultra_fast_pipeline"
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Ultra-fast settings
    file_chunk_size = 50000  # Split large files into 50K row chunks
    owner_chunk_size = 500   # Process owners in 500-row chunks for fuzzy matching
    max_workers = mp.cpu_count()  # Use all available CPU cores
    
    logger.info("Starting ultra-fast pipeline processing")
    logger.info(f"Using {max_workers} workers with {file_chunk_size} file chunks and {owner_chunk_size} owner chunks")
    
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
    
    # Create chunks directory
    chunks_dir = os.path.join(output_dir, "chunks")
    os.makedirs(chunks_dir, exist_ok=True)
    
    # Split large files into chunks
    all_chunk_files = []
    for transaction_file in transaction_files:
        file_size = os.path.getsize(transaction_file)
        file_size_mb = file_size / (1024 * 1024)
        
        if file_size_mb > 100:  # Split files larger than 100MB
            logger.info(f"Splitting large file: {os.path.basename(transaction_file)} ({file_size_mb:.1f}MB)")
            chunk_files = split_large_file(transaction_file, chunks_dir, file_chunk_size)
            all_chunk_files.extend(chunk_files)
        else:
            # Use original file for smaller files
            all_chunk_files.append(transaction_file)
    
    logger.info(f"Total chunks to process: {len(all_chunk_files)}")
    
    # Process chunks in parallel
    all_results = []
    
    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {}
            for chunk_file in all_chunk_files:
                future = executor.submit(
                    process_chunk_file,
                    owners_file=owners_file,
                    chunk_file=chunk_file,
                    output_dir=output_dir,
                    run_id=run_id,
                    chunk_size=owner_chunk_size
                )
                future_to_chunk[future] = os.path.basename(chunk_file)
            
            # Collect results
            for future in as_completed(future_to_chunk):
                chunk_name = future_to_chunk[future]
                try:
                    result = future.result()
                    all_results.append(result)
                    logger.info(f"Completed processing {chunk_name}")
                except Exception as e:
                    logger.error(f"Error processing {chunk_name}: {e}")
    else:
        # Sequential processing
        for chunk_file in all_chunk_files:
            result = process_chunk_file(
                owners_file, chunk_file, output_dir, run_id, owner_chunk_size
            )
            all_results.append(result)
    
    # Combine results
    logger.info("Combining results from all chunks")
    combined_stats = combine_ultra_results(all_results, output_dir)
    
    # Final summary
    logger.info("Ultra-fast pipeline processing completed!")
    logger.info(f"Processed {len(all_results)} chunks")
    logger.info(f"Total matches: {combined_stats.get('data_volumes', {}).get('total_matches', 0):,}")
    logger.info(f"Results saved to: {output_dir}")


if __name__ == "__main__":
    main() 
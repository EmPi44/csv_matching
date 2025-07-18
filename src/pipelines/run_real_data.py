#!/usr/bin/env python3
"""
Process real Dubai Hills data by iterating over transaction files.

This script handles large transaction files by processing them individually
and then combining the results.
"""

import pandas as pd
import os
import glob
from datetime import datetime
from loguru import logger
from matching.pipeline import run_matching_pipeline


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


def process_single_transaction_file(
    owners_file: str,
    transaction_file: str,
    output_dir: str,
    run_id: str
) -> dict:
    """
    Process a single transaction file against the owners data.
    
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
    file_output_dir = os.path.join(output_dir, f"part_{file_name.replace('.csv', '')}")
    os.makedirs(file_output_dir, exist_ok=True)
    
    try:
        results = run_matching_pipeline(
            owners_file=owners_file,
            transactions_file=transaction_file,
            output_dir=file_output_dir,
            review_dir=output_dir,  # Shared review directory
            run_id=f"{run_id}_{file_name.replace('.csv', '')}"
        )
        
        logger.info(f"Completed processing {file_name}: {results['data_volumes']['total_matches']} matches")
        return results
        
    except Exception as e:
        logger.error(f"Error processing {file_name}: {e}")
        return None


def combine_results(all_results: list, output_dir: str) -> dict:
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
        matches_file = os.path.join(file_output_dir, 'matches.parquet')
        
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
        combined_matches_path = os.path.join(output_dir, "combined_matches.parquet")
        combined_matches.to_parquet(combined_matches_path, index=False)
        logger.info(f"Combined {len(combined_matches)} total matches saved to {combined_matches_path}")
    
    # Create combined summary
    combined_stats = {
        'run_id': f"combined_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
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
    summary_path = os.path.join(output_dir, "combined_summary.md")
    with open(summary_path, 'w') as f:
        f.write(f"""# Combined Pipeline Results

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
    
    logger.info(f"Combined summary saved to {summary_path}")
    return combined_stats


def main():
    """Main function to process real data."""
    # Configure logging
    logger.add("logs/real_data_pipeline.log", rotation="1 day", retention="7 days")
    
    # Configuration
    owners_file = "data/raw/owners/20250716/Dubai Hills.xlsx - OLD.csv"
    transactions_dir = "data/raw/transactions/20250716"
    output_dir = "data/processed/real_data"
    run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info("Starting real data processing pipeline")
    
    # Check if files exist
    if not os.path.exists(owners_file):
        logger.error(f"Owners file not found: {owners_file}")
        return
    
    if not os.path.exists(transactions_dir):
        logger.error(f"Transactions directory not found: {transactions_dir}")
        return
    
    # Get transaction files
    transaction_files = get_transaction_files(transactions_dir)
    logger.info(f"Found {len(transaction_files)} transaction files to process")
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Process each transaction file
    all_results = []
    
    for i, transaction_file in enumerate(transaction_files, 1):
        logger.info(f"Processing file {i}/{len(transaction_files)}: {os.path.basename(transaction_file)}")
        
        result = process_single_transaction_file(
            owners_file=owners_file,
            transaction_file=transaction_file,
            output_dir=output_dir,
            run_id=run_id
        )
        
        if result:
            result['file_output_dir'] = os.path.join(output_dir, f"part_{os.path.basename(transaction_file).replace('.csv', '')}")
            all_results.append(result)
    
    # Combine results
    combined_stats = combine_results(all_results, output_dir)
    
    # Display final summary
    print("\n" + "="*60)
    print("REAL DATA PIPELINE RESULTS")
    print("="*60)
    print(f"Files Processed: {combined_stats.get('files_processed', 0)}")
    print(f"Total Owners: {combined_stats.get('data_volumes', {}).get('total_owners', 0):,}")
    print(f"Total Transactions: {combined_stats.get('data_volumes', {}).get('total_transactions', 0):,}")
    print(f"Total Matches: {combined_stats.get('data_volumes', {}).get('total_matches', 0):,}")
    print(f"Owner Match Rate: {combined_stats.get('match_rates', {}).get('owner_match_rate', 0):.1%}")
    print(f"Transaction Match Rate: {combined_stats.get('match_rates', {}).get('transaction_match_rate', 0):.1%}")
    print(f"Tier 1 Matches: {combined_stats.get('tier_performance', {}).get('tier1_matches', 0):,}")
    print(f"Tier 2 Matches: {combined_stats.get('tier_performance', {}).get('tier2_matches', 0):,}")
    print("="*60)
    
    logger.info("Real data processing pipeline completed")


if __name__ == "__main__":
    main() 
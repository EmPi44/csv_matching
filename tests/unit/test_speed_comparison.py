#!/usr/bin/env python3
"""
Speed comparison test between old and new pipeline.

Tests both pipelines on a small dataset to measure performance improvement.
"""

import pandas as pd
import os
import time
from datetime import datetime
from loguru import logger
from matching.pipeline import run_matching_pipeline
from matching.fast_pipeline import run_fast_pipeline


def create_test_data():
    """Create small test datasets for speed comparison."""
    logger.info("Creating test datasets")
    
    # Create test owners data
    owners_data = {
        'Project': ['Dubai Hills Estate', 'Dubai Hills Estate', 'Dubai Hills Estate'],
        'BuildingNameEn': ['Building A', 'Building B', 'Building C'],
        'UnitNumber': ['101', '202', '303'],
        ' Size ': [100.5, 150.2, 200.0],
        'NameEn': ['John Doe', 'Jane Smith', 'Bob Johnson']
    }
    
    # Create test transactions data
    transactions_data = {
        'project_name_en': ['Dubai Hills Estate', 'Dubai Hills Estate', 'Dubai Hills Estate'],
        'building_name_en': ['Building A', 'Building B', 'Building C'],
        'procedure_area': [100.0, 150.5, 200.2],
        'buyer_name': ['John Doe', 'Jane Smith', 'Bob Johnson']
    }
    
    owners_df = pd.DataFrame(owners_data)
    transactions_df = pd.DataFrame(transactions_data)
    
    # Save test data
    os.makedirs('data/test', exist_ok=True)
    owners_df.to_csv('data/test/test_owners.csv', index=False)
    transactions_df.to_csv('data/test/test_transactions.csv', index=False)
    
    logger.info(f"Created test data: {len(owners_df)} owners, {len(transactions_df)} transactions")
    return 'data/test/test_owners.csv', 'data/test/test_transactions.csv'


def test_old_pipeline(owners_file: str, transactions_file: str) -> dict:
    """Test the old pipeline and measure time."""
    logger.info("Testing old pipeline")
    
    start_time = time.time()
    
    try:
        results = run_matching_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir='data/test/old_pipeline',
            run_id='speed_test_old'
        )
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        results['execution_time'] = execution_time
        results['pipeline_type'] = 'old'
        
        logger.info(f"Old pipeline completed in {execution_time:.2f} seconds")
        return results
        
    except Exception as e:
        logger.error(f"Old pipeline failed: {e}")
        return {'error': str(e), 'pipeline_type': 'old'}


def test_fast_pipeline(owners_file: str, transactions_file: str) -> dict:
    """Test the fast pipeline and measure time."""
    logger.info("Testing fast pipeline")
    
    start_time = time.time()
    
    try:
        results = run_fast_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir='data/test/fast_pipeline',
            run_id='speed_test_fast'
        )
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        results['execution_time'] = execution_time
        results['pipeline_type'] = 'fast'
        
        logger.info(f"Fast pipeline completed in {execution_time:.2f} seconds")
        return results
        
    except Exception as e:
        logger.error(f"Fast pipeline failed: {e}")
        return {'error': str(e), 'pipeline_type': 'fast'}


def compare_results(old_results: dict, fast_results: dict):
    """Compare results from both pipelines."""
    logger.info("Comparing pipeline results")
    
    print("\n" + "="*60)
    print("SPEED COMPARISON RESULTS")
    print("="*60)
    
    # Execution time comparison
    if 'execution_time' in old_results and 'execution_time' in fast_results:
        old_time = old_results['execution_time']
        fast_time = fast_results['execution_time']
        speedup = old_time / fast_time if fast_time > 0 else float('inf')
        
        print(f"\nExecution Time:")
        print(f"  Old Pipeline:    {old_time:.2f} seconds")
        print(f"  Fast Pipeline:   {fast_time:.2f} seconds")
        print(f"  Speedup:         {speedup:.1f}x faster")
    
    # Match results comparison
    if 'data_volumes' in old_results and 'data_volumes' in fast_results:
        old_matches = old_results['data_volumes'].get('total_matches', 0)
        fast_matches = fast_results['data_volumes'].get('total_matches', 0)
        
        print(f"\nMatch Results:")
        print(f"  Old Pipeline:    {old_matches} matches")
        print(f"  Fast Pipeline:   {fast_matches} matches")
        print(f"  Difference:      {abs(old_matches - fast_matches)} matches")
    
    # Tier performance comparison
    if 'tier_performance' in old_results and 'tier_performance' in fast_results:
        old_tier1 = old_results['tier_performance'].get('tier1_matches', 0)
        old_tier2 = old_results['tier_performance'].get('tier2_matches', 0)
        fast_tier1 = fast_results['tier_performance'].get('tier1_matches', 0)
        fast_tier2 = fast_results['tier_performance'].get('tier2_matches', 0)
        
        print(f"\nTier Performance:")
        print(f"  Old Pipeline - Tier 1: {old_tier1}, Tier 2: {old_tier2}")
        print(f"  Fast Pipeline - Tier 1: {fast_tier1}, Tier 2: {fast_tier2}")
    
    print("\n" + "="*60)


def main():
    """Main function to run speed comparison."""
    logger.info("Starting speed comparison test")
    
    # Create test data
    owners_file, transactions_file = create_test_data()
    
    # Test old pipeline
    old_results = test_old_pipeline(owners_file, transactions_file)
    
    # Test fast pipeline
    fast_results = test_fast_pipeline(owners_file, transactions_file)
    
    # Compare results
    compare_results(old_results, fast_results)
    
    logger.info("Speed comparison test completed")


if __name__ == "__main__":
    main() 
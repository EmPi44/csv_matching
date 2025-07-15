#!/usr/bin/env python3
"""
Example usage of the Dubai Hills property matching pipeline.

This script demonstrates how to use the matching pipeline with sample data.
"""

import pandas as pd
import os
import sys
from loguru import logger

# Add the current directory to the path
sys.path.insert(0, os.path.dirname(__file__))

from matching.pipeline import run_matching_pipeline


def create_sample_data():
    """
    Create sample owner and transaction data for testing.
    
    Returns:
        Tuple of (owners_df, transactions_df)
    """
    logger.info("Creating sample data for testing")
    
    # Sample owner data
    owners_data = {
        'project': ['Dubai Hills', 'Dubai Hills', 'Dubai Hills', 'Dubai Hills', 'Dubai Hills'],
        'building': ['Tower A', 'Building 1', 'BLDG 2', 'Tower B', 'Block 3'],
        'unit_number': ['Apt 101', 'Unit 205', '123', 'Apt 301', 'Unit 405'],
        'area': [120.5, 150.0, 200.0, 180.0, 250.0],
        'owner_name': ['John Smith', 'Jane Doe', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson']
    }
    
    # Sample transaction data (some matching, some not)
    transactions_data = {
        'project': ['Dubai Hills', 'Dubai Hills', 'Dubai Hills', 'Dubai Hills', 'Dubai Hills', 'Dubai Hills'],
        'building': ['Tower A', 'Building 1', 'BLDG 2', 'Tower C', 'Tower B', 'Block 4'],
        'unit_number': ['Apt 101', 'Unit 205', '123', 'Apt 401', 'Apt 301', 'Unit 505'],
        'area': [120.0, 150.5, 200.0, 300.0, 180.0, 350.0],
        'buyer_name': ['John Smith', 'Jane Doe', 'Bob Johnson', 'David Lee', 'Alice Brown', 'Eve Davis']
    }
    
    owners_df = pd.DataFrame(owners_data)
    transactions_df = pd.DataFrame(transactions_data)
    
    logger.info(f"Created {len(owners_df)} owner records and {len(transactions_df)} transaction records")
    
    return owners_df, transactions_df


def main():
    """Main function to run the example."""
    # Configure logging
    logger.add("logs/matching_pipeline.log", rotation="1 day", retention="7 days")
    
    logger.info("Starting Dubai Hills matching pipeline example")
    
    # Create sample data
    owners_df, transactions_df = create_sample_data()
    
    # Save sample data to files
    os.makedirs("data/raw/owners/20250716", exist_ok=True)
    os.makedirs("data/raw/transactions/20250716", exist_ok=True)
    
    owners_file = "data/raw/owners/20250716/sample_owners.csv"
    transactions_file = "data/raw/transactions/20250716/sample_transactions.csv"
    
    owners_df.to_csv(owners_file, index=False)
    transactions_df.to_csv(transactions_file, index=False)
    
    logger.info(f"Sample data saved to {owners_file} and {transactions_file}")
    
    try:
        # Run the matching pipeline
        logger.info("Running matching pipeline...")
        results = run_matching_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir="data/processed",
            review_dir="data/review"
        )
        
        # Display results
        logger.info("Pipeline completed successfully!")
        logger.info(f"Pipeline results: {results}")
        
        # Show key metrics
        data_volumes = results['data_volumes']
        match_rates = results['match_rates']
        
        print("\n" + "="*50)
        print("PIPELINE RESULTS")
        print("="*50)
        print(f"Total Owners: {data_volumes['total_owners']}")
        print(f"Total Transactions: {data_volumes['total_transactions']}")
        print(f"Total Matches: {data_volumes['total_matches']}")
        print(f"Owner Match Rate: {match_rates['owner_match_rate']:.1%}")
        print(f"Transaction Match Rate: {match_rates['transaction_match_rate']:.1%}")
        print(f"Tier 1 Matches: {results['tier_performance']['tier1_matches']}")
        print(f"Tier 2 Matches: {results['tier_performance']['tier2_matches']}")
        print("="*50)
        
        # Check if output files were created
        output_files = [
            "data/processed/matches.parquet",
            "data/processed/qa_report.md"
        ]
        
        print("\nOutput files created:")
        for file_path in output_files:
            if os.path.exists(file_path):
                print(f"✓ {file_path}")
            else:
                print(f"✗ {file_path} (not found)")
        
        print(f"\nLog file: logs/matching_pipeline.log")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main() 
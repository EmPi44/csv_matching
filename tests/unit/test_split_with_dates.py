#!/usr/bin/env python3
"""
Test script for splitting CSV files with date normalization.
"""

import pandas as pd
import os
from csv_processor import CSVProcessor
from loguru import logger

def test_split_with_date_normalization():
    """Test the new splitting with date normalization functionality."""
    
    print("🧪 Testing CSV splitting with date normalization...")
    
    # Create a test CSV file with dates in DD-MM-YYYY format
    test_data = {
        'transaction_id': [f'txn_{i:03d}' for i in range(1, 21)],
        'instance_date': [
            '19-01-2009', '24-06-2025', '01-11-2016', '2009-01-19', '2025-06-24',
            '01-11-2016', '19-01-2009', '24-06-2025', '01-11-2016', '2009-01-19',
            '2025-06-24', '01-11-2016', '19-01-2009', '24-06-2025', '01-11-2016',
            '2009-01-19', '2025-06-24', '01-11-2016', '19-01-2009', '24-06-2025'
        ],
        'building_name': [f'Building {i}' for i in range(1, 21)],
        'area': [100 + i * 10 for i in range(20)]
    }
    
    test_df = pd.DataFrame(test_data)
    test_file = 'test_transactions.csv'
    test_output_dir = 'test_split_output'
    
    # Save test file
    test_df.to_csv(test_file, index=False)
    print(f"📁 Created test file: {test_file}")
    print(f"📊 Test data preview:")
    print(test_df.head().to_string(index=False))
    
    try:
        # Initialize processor
        processor = CSVProcessor(chunk_size=5)  # Small chunk size for testing
        
        # Test splitting with date normalization
        result = processor.split_file_with_date_normalization(
            input_file=test_file,
            output_dir=test_output_dir,
            max_size_mb=0.001,  # Very small size to force multiple splits
            date_columns=['instance_date']
        )
        
        print(f"\n✅ Splitting completed: {result}")
        
        # Check the results
        print(f"\n📋 Created files:")
        for file_info in result['split_files']:
            print(f"  - {file_info['file_path']}")
            print(f"    Rows: {file_info['rows']}, Size: {file_info['size_mb']} MB")
            
            # Read and check the file
            split_df = pd.read_csv(file_info['file_path'])
            print(f"    Date sample: {split_df['instance_date'].iloc[0]}")
        
        print(f"\n📅 Date columns normalized: {result['date_columns_normalized']}")
        
    except Exception as e:
        print(f"❌ Error during testing: {e}")
    
    finally:
        # Clean up test files
        if os.path.exists(test_file):
            os.remove(test_file)
            print(f"🗑️  Cleaned up: {test_file}")
        
        if os.path.exists(test_output_dir):
            import shutil
            shutil.rmtree(test_output_dir)
            print(f"🗑️  Cleaned up: {test_output_dir}")

def main():
    """Main execution function."""
    
    logger.info("🎯 Testing CSV Splitting with Date Normalization")
    logger.info("=" * 50)
    
    test_split_with_date_normalization()
    
    logger.info("🎉 Test completed!")

if __name__ == "__main__":
    main() 
#!/usr/bin/env python3
"""
Test script for date normalization functionality.
"""

import pandas as pd
import sys
import os
from csv_processor import CSVProcessor
from matching.preprocess import normalize_date

def test_date_normalization():
    """Test the date normalization function with various date formats."""
    
    print("🧪 Testing date normalization functionality...")
    
    # Test data with various date formats
    test_dates = [
        "19-01-2009",      # DD-MM-YYYY (should become 2009-01-19)
        "24-06-2025",      # DD-MM-YYYY (should become 2025-06-24)
        "01-11-2016",      # DD-MM-YYYY (should become 2016-11-01)
        "2009-01-19",      # YYYY-MM-DD (already correct)
        "2025-06-24",      # YYYY-MM-DD (already correct)
        "01/15/2020",      # MM/DD/YYYY (should become 2020-01-15)
        "15/01/2020",      # DD/MM/YYYY (should become 2020-01-15)
        "",                # Empty string
        None,              # None value
        "invalid-date",    # Invalid date
        "2020-13-45",      # Invalid date
    ]
    
    print("\n📅 Testing individual date normalization:")
    for date_val in test_dates:
        normalized = normalize_date(date_val)
        print(f"  '{date_val}' -> '{normalized}'")
    
    # Test with pandas Series
    print("\n📊 Testing pandas Series normalization:")
    processor = CSVProcessor()
    date_series = pd.Series(test_dates)
    normalized_series = processor.normalize_date_column(date_series)
    
    for original, normalized in zip(test_dates, normalized_series):
        print(f"  '{original}' -> '{normalized}'")
    
    print("\n✅ Date normalization test completed!")

def test_csv_processing():
    """Test CSV processing with date normalization."""
    
    print("\n🔄 Testing CSV processing with date normalization...")
    
    # Create a small test CSV file
    test_data = {
        'transaction_id': ['1', '2', '3', '4'],
        'instance_date': ['19-01-2009', '24-06-2025', '01-11-2016', '2009-01-19'],
        'building_name': ['Tower A', 'Tower B', 'Tower C', 'Tower D'],
        'area': [100, 150, 200, 120]
    }
    
    test_df = pd.DataFrame(test_data)
    test_file = 'test_dates.csv'
    test_output = 'test_dates_normalized.csv'
    
    # Save test file
    test_df.to_csv(test_file, index=False)
    print(f"📁 Created test file: {test_file}")
    
    try:
        # Process with date normalization
        processor = CSVProcessor(chunk_size=2)
        result = processor.process_with_date_normalization(
            input_file=test_file,
            output_file=test_output,
            date_columns=['instance_date']
        )
        
        print(f"✅ Processing completed: {result}")
        
        # Read and display results
        result_df = pd.read_csv(test_output)
        print("\n📋 Results:")
        print(result_df.to_string(index=False))
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
    
    finally:
        # Clean up test files
        for file in [test_file, test_output]:
            if os.path.exists(file):
                os.remove(file)
                print(f"🗑️  Cleaned up: {file}")

if __name__ == "__main__":
    test_date_normalization()
    test_csv_processing()
    print("\n🎉 All tests completed!") 
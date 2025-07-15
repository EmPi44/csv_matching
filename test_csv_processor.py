#!/usr/bin/env python3
"""
Test script for the CSV processor functionality.
Creates a sample CSV file and tests the processing operations.
"""

import pandas as pd
import os
import tempfile
import shutil
from csv_processor import CSVProcessor

def create_test_csv(file_path: str, rows: int = 1000):
    """Create a test CSV file with sample data."""
    print(f"Creating test CSV file with {rows:,} rows...")
    
    # Generate sample data
    data = {
        'id': range(1, rows + 1),
        'name': [f'User_{i}' for i in range(1, rows + 1)],
        'email': [f'user_{i}@example.com' for i in range(1, rows + 1)],
        'age': [20 + (i % 50) for i in range(1, rows + 1)],
        'city': [f'City_{i % 10}' for i in range(1, rows + 1)],
        'unused_column': [f'unused_data_{i}' for i in range(1, rows + 1)],
        'metadata_field': [f'metadata_{i}' for i in range(1, rows + 1)],
        'timestamp': pd.date_range('2023-01-01', periods=rows, freq='H')
    }
    
    df = pd.DataFrame(data)
    df.to_csv(file_path, index=False)
    
    file_size = os.path.getsize(file_path) / (1024 * 1024)
    print(f"Created {file_path} ({file_size:.2f} MB)")
    return df

def test_csv_info():
    """Test getting CSV file information."""
    print("\n=== Testing CSV Info ===")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        test_file = f.name
    
    try:
        # Create test file
        create_test_csv(test_file, 1000)
        
        # Test CSV processor
        processor = CSVProcessor()
        info = processor.get_csv_info(test_file)
        
        print(f"File size: {info['file_size_mb']:.2f} MB")
        print(f"Total rows: {info['total_rows']:,}")
        print(f"Columns: {info['columns']}")
        print(f"Column count: {info['column_count']}")
        
        assert info['total_rows'] == 1000
        assert info['column_count'] == 8
        print("✓ CSV info test passed")
        
    finally:
        os.unlink(test_file)

def test_column_removal():
    """Test column removal functionality."""
    print("\n=== Testing Column Removal ===")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        test_file = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        output_file = f.name
    
    try:
        # Create test file
        create_test_csv(test_file, 1000)
        
        # Test column removal
        processor = CSVProcessor()
        columns_to_remove = ['unused_column', 'metadata_field']
        
        result = processor.remove_columns(test_file, output_file, columns_to_remove)
        
        print(f"Columns removed: {result['columns_removed']}")
        print(f"Columns kept: {result['columns_kept']}")
        print(f"Output size: {result['output_size_mb']:.2f} MB")
        print(f"Rows processed: {result['total_rows_processed']:,}")
        
        # Verify the output file
        df_output = pd.read_csv(output_file)
        print(f"Output columns: {list(df_output.columns)}")
        
        assert len(df_output.columns) == 6  # 8 - 2 removed
        assert 'unused_column' not in df_output.columns
        assert 'metadata_field' not in df_output.columns
        assert len(df_output) == 1000
        
        print("✓ Column removal test passed")
        
    finally:
        os.unlink(test_file)
        os.unlink(output_file)

def test_file_splitting():
    """Test file splitting functionality."""
    print("\n=== Testing File Splitting ===")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        test_file = f.name
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create test file
        create_test_csv(test_file, 5000)
        
        # Test file splitting
        processor = CSVProcessor()
        max_size_mb = 0.1  # Small size to force multiple splits
        
        result = processor.split_file_by_size(test_file, temp_dir, max_size_mb)
        
        print(f"Files created: {result['total_files_created']}")
        print(f"Total rows processed: {result['total_rows_processed']:,}")
        print(f"Output directory: {result['output_directory']}")
        
        # Check split files
        for split_file in result['split_files']:
            print(f"  {split_file['file_path']}")
            print(f"    - Rows: {split_file['rows']:,}")
            print(f"    - Size: {split_file['size_mb']:.2f} MB")
            
            # Verify each split file
            df_split = pd.read_csv(split_file['file_path'])
            assert len(df_split) == split_file['rows']
        
        assert result['total_rows_processed'] == 5000
        assert result['total_files_created'] > 1
        
        print("✓ File splitting test passed")
        
    finally:
        os.unlink(test_file)
        shutil.rmtree(temp_dir)

def test_combined_operation():
    """Test combined operation (remove columns + split)."""
    print("\n=== Testing Combined Operation ===")
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        test_file = f.name
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create test file
        create_test_csv(test_file, 2000)
        
        # Test combined operation
        processor = CSVProcessor()
        columns_to_remove = ['unused_column', 'metadata_field']
        max_size_mb = 0.1
        
        result = processor.process_and_split(
            test_file, 
            temp_dir, 
            columns_to_remove, 
            max_size_mb
        )
        
        print("Column removal results:")
        print(f"  - Columns removed: {result['column_removal']['columns_removed']}")
        print(f"  - Output size: {result['column_removal']['output_size_mb']:.2f} MB")
        
        print("\nFile splitting results:")
        print(f"  - Files created: {result['file_splitting']['total_files_created']}")
        print(f"  - Output directory: {result['final_output_directory']}")
        
        # Verify results
        assert result['column_removal']['total_rows_processed'] == 2000
        assert result['file_splitting']['total_rows_processed'] == 2000
        
        print("✓ Combined operation test passed")
        
    finally:
        os.unlink(test_file)
        shutil.rmtree(temp_dir)

def main():
    """Run all tests."""
    print("Starting CSV Processor Tests...")
    
    try:
        test_csv_info()
        test_column_removal()
        test_file_splitting()
        test_combined_operation()
        
        print("\n🎉 All tests passed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise

if __name__ == "__main__":
    main() 
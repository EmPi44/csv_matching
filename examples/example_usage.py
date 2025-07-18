#!/usr/bin/env python3
"""
Example usage of the CSV processing functionality.
This script demonstrates how to use the CSVProcessor class directly.
"""

import os
from csv_processor import CSVProcessor

def main():
    """Example usage of the CSV processor."""
    
    # Initialize the processor
    processor = CSVProcessor(chunk_size=10000)
    
    # Example file path (replace with your actual file path)
    input_file = "your_large_file.csv"
    output_dir = "processed_files"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file {input_file} not found. Please update the file path.")
        return
    
    print("=== CSV Processing Example ===\n")
    
    # Step 1: Get file information
    print("1. Getting file information...")
    try:
        file_info = processor.get_csv_info(input_file)
        print(f"   File size: {file_info['file_size_mb']} MB")
        print(f"   Total rows: {file_info['total_rows']:,}")
        print(f"   Columns: {file_info['column_count']}")
        print(f"   Column names: {file_info['columns']}")
        print()
    except Exception as e:
        print(f"   Error getting file info: {e}")
        return
    
    # Step 2: Example columns to remove (update based on your data)
    columns_to_remove = [
        # Add your irrelevant column names here
        # Example: "unused_column", "metadata_field", etc.
    ]
    
    if columns_to_remove:
        print("2. Removing irrelevant columns...")
        try:
            output_file = os.path.join(output_dir, "cleaned_data.csv")
            result = processor.remove_columns(input_file, output_file, columns_to_remove)
            print(f"   Columns removed: {result['columns_removed']}")
            print(f"   Columns kept: {result['columns_kept']}")
            print(f"   Output size: {result['output_size_mb']} MB")
            print(f"   Rows processed: {result['total_rows_processed']:,}")
            print()
            
            # Use the cleaned file for splitting
            input_file = output_file
        except Exception as e:
            print(f"   Error removing columns: {e}")
            return
    
    # Step 3: Split the file
    print("3. Splitting file into smaller chunks...")
    try:
        max_size_mb = 50  # Maximum 50MB per file
        result = processor.split_file_by_size(input_file, output_dir, max_size_mb)
        
        print(f"   Files created: {result['total_files_created']}")
        print(f"   Total rows processed: {result['total_rows_processed']:,}")
        print(f"   Output directory: {result['output_directory']}")
        print()
        
        print("   Split files:")
        for split_file in result['split_files']:
            print(f"     {split_file['file_path']}")
            print(f"       - Rows: {split_file['rows']:,}")
            print(f"       - Size: {split_file['size_mb']} MB")
        print()
        
    except Exception as e:
        print(f"   Error splitting file: {e}")
        return
    
    print("=== Processing Complete ===")
    print(f"All processed files are saved in: {output_dir}")

def example_combined_operation():
    """Example of the combined operation (remove columns + split)."""
    
    processor = CSVProcessor(chunk_size=10000)
    
    input_file = "your_large_file.csv"
    output_dir = "processed_files"
    columns_to_remove = [
        # Add your irrelevant column names here
    ]
    max_size_mb = 50
    
    print("=== Combined Operation Example ===")
    
    try:
        result = processor.process_and_split(
            input_file, 
            output_dir, 
            columns_to_remove, 
            max_size_mb
        )
        
        print("Column removal results:")
        print(f"  - Columns removed: {result['column_removal']['columns_removed']}")
        print(f"  - Output size: {result['column_removal']['output_size_mb']} MB")
        
        print("\nFile splitting results:")
        print(f"  - Files created: {result['file_splitting']['total_files_created']}")
        print(f"  - Output directory: {result['final_output_directory']}")
        
    except Exception as e:
        print(f"Error in combined operation: {e}")

if __name__ == "__main__":
    # Run the main example
    main()
    
    # Uncomment to run the combined operation example
    # example_combined_operation() 
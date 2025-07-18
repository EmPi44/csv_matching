#!/usr/bin/env python3
"""
Split transaction data with date normalization during splitting process.
"""

import os
import sys
from pathlib import Path
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'core'))
from csv_processor import CSVProcessor
from loguru import logger

def split_transactions_with_date_normalization():
    """Split transaction data with date normalization."""
    
    logger.info("🚀 Starting transaction splitting with date normalization...")
    
    # Input file path
    input_file = "corrected_transactions.csv"
    
    # Check if input file exists
    if not os.path.exists(input_file):
        logger.error(f"❌ Input file not found: {input_file}")
        logger.info("Available files:")
        for file in os.listdir("."):
            if file.endswith(".csv"):
                logger.info(f"  - {file}")
        return False
    
    # Output directory for split files
    output_dir = "data/raw/transactions/20250716/split_with_dates"
    
    # Date columns to normalize
    date_columns = ['instance_date']
    
    try:
        # Initialize processor
        processor = CSVProcessor(chunk_size=10000)
        
        # Get file info
        file_info = processor.get_csv_info(input_file)
        logger.info(f"📊 Processing file: {input_file}")
        logger.info(f"   Size: {file_info['file_size_mb']} MB")
        logger.info(f"   Rows: {file_info['total_rows']:,}")
        logger.info(f"   Columns: {file_info['column_count']}")
        
        # Split with date normalization
        logger.info("🔄 Starting splitting with date normalization...")
        result = processor.split_file_with_date_normalization(
            input_file=input_file,
            output_dir=output_dir,
            max_size_mb=50.0,  # 50MB per file
            date_columns=date_columns
        )
        
        # Display results
        logger.info("✅ Splitting completed successfully!")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"📊 Total files created: {result['total_files_created']}")
        logger.info(f"📊 Total rows processed: {result['total_rows_processed']:,}")
        logger.info(f"📅 Date columns normalized: {result['date_columns_normalized']}")
        
        # List created files
        logger.info("📋 Created files:")
        for file_info in result['split_files']:
            logger.info(f"  - {file_info['file_path']}")
            logger.info(f"    Rows: {file_info['rows']:,}, Size: {file_info['size_mb']} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during splitting: {e}")
        return False

def verify_date_normalization():
    """Verify that dates were properly normalized in the split files."""
    
    logger.info("🔍 Verifying date normalization...")
    
    output_dir = "data/raw/transactions/20250716/split_with_dates"
    
    if not os.path.exists(output_dir):
        logger.error(f"❌ Output directory not found: {output_dir}")
        return False
    
    # Get list of split files
    split_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
    split_files.sort()
    
    logger.info(f"📁 Found {len(split_files)} split files to verify")
    
    for i, file in enumerate(split_files[:3], 1):  # Check first 3 files
        file_path = os.path.join(output_dir, file)
        
        try:
            # Read first few rows to check dates
            df_sample = pd.read_csv(file_path, nrows=5)
            
            if 'instance_date' in df_sample.columns:
                logger.info(f"📅 File {i}: {file}")
                logger.info(f"   Sample dates: {df_sample['instance_date'].tolist()}")
                
                # Check if dates are in YYYY-MM-DD format
                date_format_ok = all(
                    re.match(r'\d{4}-\d{2}-\d{2}', str(date)) 
                    for date in df_sample['instance_date'] 
                    if pd.notna(date) and str(date).strip()
                )
                
                if date_format_ok:
                    logger.info(f"   ✅ Dates are properly normalized")
                else:
                    logger.warning(f"   ⚠️  Some dates may not be normalized")
            else:
                logger.warning(f"   ⚠️  No instance_date column found in {file}")
                
        except Exception as e:
            logger.error(f"   ❌ Error reading {file}: {e}")
    
    return True

def main():
    """Main execution function."""
    
    logger.info("🎯 Transaction Splitting with Date Normalization")
    logger.info("=" * 50)
    
    # Step 1: Split with date normalization
    success = split_transactions_with_date_normalization()
    if not success:
        logger.error("❌ Failed to split transactions with date normalization")
        return
    
    # Step 2: Verify date normalization
    success = verify_date_normalization()
    if not success:
        logger.error("❌ Failed to verify date normalization")
        return
    
    logger.info("🎉 All processing completed successfully!")
    logger.info("📁 Split files with normalized dates are ready for use")

if __name__ == "__main__":
    import pandas as pd
    import re
    main() 
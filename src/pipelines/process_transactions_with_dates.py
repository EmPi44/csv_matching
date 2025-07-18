#!/usr/bin/env python3
"""
Process transaction data with date normalization to fix DD-MM-YYYY format issues.
"""

import os
import sys
from pathlib import Path
from csv_processor import CSVProcessor
from loguru import logger

def process_transactions_with_date_normalization():
    """Process transaction data with date normalization."""
    
    logger.info("🚀 Starting transaction processing with date normalization...")
    
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
    
    # Output file path
    output_file = "transactions_with_normalized_dates.csv"
    
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
        
        # Process with date normalization
        logger.info("🔄 Starting date normalization...")
        result = processor.process_with_date_normalization(
            input_file=input_file,
            output_file=output_file,
            date_columns=date_columns
        )
        
        # Display results
        logger.info("✅ Processing completed successfully!")
        logger.info(f"📁 Output file: {output_file}")
        logger.info(f"📊 Processed {result['total_rows_processed']:,} rows")
        logger.info(f"📊 Processed {result['chunks_processed']} chunks")
        logger.info(f"📊 Output size: {result['output_size_mb']} MB")
        logger.info(f"📅 Date columns normalized: {result['date_columns_normalized']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during processing: {e}")
        return False

def split_normalized_transactions():
    """Split the normalized transaction file into smaller chunks."""
    
    logger.info("🔪 Starting file splitting...")
    
    input_file = "transactions_with_normalized_dates.csv"
    
    if not os.path.exists(input_file):
        logger.error(f"❌ Normalized file not found: {input_file}")
        return False
    
    # Output directory for split files
    output_dir = "data/raw/transactions/20250716/normalized_split"
    
    try:
        # Initialize processor
        processor = CSVProcessor(chunk_size=10000)
        
        # Split file into 50MB chunks
        max_size_mb = 50.0
        
        result = processor.split_file_by_size(
            input_file=input_file,
            output_dir=output_dir,
            max_size_mb=max_size_mb
        )
        
        # Display results
        logger.info("✅ File splitting completed successfully!")
        logger.info(f"📁 Output directory: {output_dir}")
        logger.info(f"📊 Total files created: {result['total_files_created']}")
        logger.info(f"📊 Total rows processed: {result['total_rows_processed']:,}")
        
        # List created files
        logger.info("📋 Created files:")
        for file_info in result['split_files']:
            logger.info(f"  - {file_info['file_path']}")
            logger.info(f"    Rows: {file_info['rows']:,}, Size: {file_info['size_mb']} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error during file splitting: {e}")
        return False

def main():
    """Main execution function."""
    
    logger.info("🎯 Transaction Processing with Date Normalization")
    logger.info("=" * 50)
    
    # Step 1: Process with date normalization
    success = process_transactions_with_date_normalization()
    if not success:
        logger.error("❌ Failed to process transactions with date normalization")
        return
    
    # Step 2: Split into smaller files
    success = split_normalized_transactions()
    if not success:
        logger.error("❌ Failed to split normalized transactions")
        return
    
    logger.info("🎉 All processing completed successfully!")
    logger.info("📁 Normalized files are ready for pipeline processing")

if __name__ == "__main__":
    main() 
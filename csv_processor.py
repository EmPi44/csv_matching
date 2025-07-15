import pandas as pd
import os
from typing import List, Dict, Optional
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVProcessor:
    """Handles large CSV file processing with column removal and file splitting."""
    
    def __init__(self, chunk_size: int = 10000):
        """
        Initialize the CSV processor.
        
        Args:
            chunk_size: Number of rows to process in each chunk for memory efficiency
        """
        self.chunk_size = chunk_size
    
    def get_csv_info(self, file_path: str) -> Dict:
        """
        Get information about the CSV file without loading it entirely into memory.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            Dictionary containing file information
        """
        try:
            # Get file size
            file_size = os.path.getsize(file_path)
            file_size_mb = file_size / (1024 * 1024)
            
            # Read just the header to get column information
            df_sample = pd.read_csv(file_path, nrows=0)
            columns = list(df_sample.columns)
            
            # Count total rows efficiently
            total_rows = sum(1 for _ in open(file_path)) - 1  # Subtract header row
            
            return {
                "file_size_mb": round(file_size_mb, 2),
                "total_rows": total_rows,
                "columns": columns,
                "column_count": len(columns)
            }
        except Exception as e:
            logger.error(f"Error getting CSV info: {e}")
            raise
    
    def remove_columns(self, input_file: str, output_file: str, columns_to_remove: List[str]) -> Dict:
        """
        Remove specified columns from CSV file and save to new file.
        
        Args:
            input_file: Path to input CSV file
            output_file: Path to output CSV file
            columns_to_remove: List of column names to remove
            
        Returns:
            Dictionary with processing results
        """
        try:
            logger.info(f"Starting column removal process for {input_file}")
            
            # Read the first chunk to get column information
            first_chunk = pd.read_csv(input_file, nrows=self.chunk_size)
            available_columns = list(first_chunk.columns)
            
            # Validate columns to remove
            invalid_columns = [col for col in columns_to_remove if col not in available_columns]
            if invalid_columns:
                raise ValueError(f"Columns not found in CSV: {invalid_columns}")
            
            # Get columns to keep
            columns_to_keep = [col for col in available_columns if col not in columns_to_remove]
            
            # Process file in chunks
            chunks_processed = 0
            total_rows_processed = 0
            
            # Write header first
            header_df = pd.read_csv(input_file, nrows=0)
            header_df[columns_to_keep].to_csv(output_file, index=False)
            
            # Process chunks
            for chunk in pd.read_csv(input_file, chunksize=self.chunk_size):
                chunk[columns_to_keep].to_csv(
                    output_file, 
                    mode='a', 
                    header=False, 
                    index=False
                )
                chunks_processed += 1
                total_rows_processed += len(chunk)
                
                if chunks_processed % 10 == 0:
                    logger.info(f"Processed {chunks_processed} chunks, {total_rows_processed} rows")
            
            # Get output file info
            output_size = os.path.getsize(output_file) / (1024 * 1024)
            
            return {
                "success": True,
                "input_file": input_file,
                "output_file": output_file,
                "columns_removed": columns_to_remove,
                "columns_kept": columns_to_keep,
                "chunks_processed": chunks_processed,
                "total_rows_processed": total_rows_processed,
                "output_size_mb": round(output_size, 2)
            }
            
        except Exception as e:
            logger.error(f"Error in column removal: {e}")
            raise
    
    def split_file_by_size(self, input_file: str, output_dir: str, max_size_mb: float) -> Dict:
        """
        Split CSV file into smaller files based on size limit.
        
        Args:
            input_file: Path to input CSV file
            output_dir: Directory to save split files
            max_size_mb: Maximum size in MB for each split file
            
        Returns:
            Dictionary with splitting results
        """
        try:
            logger.info(f"Starting file splitting process for {input_file}")
            
            # Create output directory if it doesn't exist
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Get file info
            file_info = self.get_csv_info(input_file)
            total_rows = file_info["total_rows"]
            
            # Calculate approximate rows per file based on size
            file_size_mb = file_info["file_size_mb"]
            rows_per_file = int((max_size_mb / file_size_mb) * total_rows)
            
            # Ensure minimum chunk size
            rows_per_file = max(rows_per_file, self.chunk_size)
            
            # Split file
            file_number = 1
            total_rows_processed = 0
            split_files = []
            
            # Read header for all files
            header_df = pd.read_csv(input_file, nrows=0)
            
            for chunk in pd.read_csv(input_file, chunksize=rows_per_file):
                # Create output filename
                base_name = Path(input_file).stem
                output_file = os.path.join(output_dir, f"{base_name}_part_{file_number:03d}.csv")
                
                # Write chunk to file
                chunk.to_csv(output_file, index=False)
                
                split_files.append({
                    "file_path": output_file,
                    "rows": len(chunk),
                    "size_mb": round(os.path.getsize(output_file) / (1024 * 1024), 2)
                })
                
                total_rows_processed += len(chunk)
                file_number += 1
                
                logger.info(f"Created split file {output_file} with {len(chunk)} rows")
            
            return {
                "success": True,
                "input_file": input_file,
                "output_directory": output_dir,
                "split_files": split_files,
                "total_files_created": len(split_files),
                "total_rows_processed": total_rows_processed,
                "max_size_mb": max_size_mb
            }
            
        except Exception as e:
            logger.error(f"Error in file splitting: {e}")
            raise
    
    def process_and_split(self, input_file: str, output_dir: str, columns_to_remove: List[str], 
                         max_size_mb: float) -> Dict:
        """
        Combined operation: remove columns and then split the file.
        
        Args:
            input_file: Path to input CSV file
            output_dir: Directory to save processed files
            columns_to_remove: List of column names to remove
            max_size_mb: Maximum size in MB for each split file
            
        Returns:
            Dictionary with processing results
        """
        try:
            logger.info(f"Starting combined process and split operation")
            
            # Create temporary file for column removal
            temp_file = os.path.join(output_dir, "temp_processed.csv")
            
            # Step 1: Remove columns
            column_removal_result = self.remove_columns(input_file, temp_file, columns_to_remove)
            
            # Step 2: Split the processed file
            split_result = self.split_file_by_size(temp_file, output_dir, max_size_mb)
            
            # Clean up temporary file
            if os.path.exists(temp_file):
                os.remove(temp_file)
            
            return {
                "success": True,
                "column_removal": column_removal_result,
                "file_splitting": split_result,
                "final_output_directory": output_dir
            }
            
        except Exception as e:
            logger.error(f"Error in combined process: {e}")
            raise 
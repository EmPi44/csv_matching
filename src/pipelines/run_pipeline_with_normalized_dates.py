#!/usr/bin/env python3
"""
Run the matching pipeline with normalized date files.
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from loguru import logger

# Add the matching module to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'matching'))

from matching.fast_pipeline import run_fast_pipeline
from matching.preprocess import preprocess_transactions, get_data_info

def run_pipeline_with_normalized_dates():
    """Run the pipeline with normalized date files."""
    
    logger.info("🚀 Starting pipeline with normalized dates...")
    
    # Input directory with normalized files
    input_dir = "data/raw/transactions/20250716/normalized_split"
    
    if not os.path.exists(input_dir):
        logger.error(f"❌ Input directory not found: {input_dir}")
        return False
    
    # Owners file
    owners_file = "data/test/test_owners.csv"
    
    if not os.path.exists(owners_file):
        logger.error(f"❌ Owners file not found: {owners_file}")
        return False
    
    logger.info(f"📁 Using owners file: {owners_file}")
    
    # Get list of normalized files
    normalized_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    normalized_files.sort()
    
    logger.info(f"📁 Found {len(normalized_files)} normalized files:")
    for file in normalized_files:
        file_path = os.path.join(input_dir, file)
        file_size = os.path.getsize(file_path) / (1024 * 1024)
        logger.info(f"  - {file} ({file_size:.2f} MB)")
    
    # Output directory for results
    output_dir = "data/processed/normalized_pipeline"
    os.makedirs(output_dir, exist_ok=True)
    
    # Run pipeline for each file
    all_results = []
    
    for i, file in enumerate(normalized_files, 1):
        logger.info(f"\n🔄 Processing file {i}/{len(normalized_files)}: {file}")
        
        input_file = os.path.join(input_dir, file)
        
        try:
            # Run fast pipeline
            result = run_fast_pipeline(
                owners_file=owners_file,
                transactions_file=input_file,
                output_dir=output_dir,
                run_id=f"normalized_{i}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )
            
            all_results.append({
                'file': file,
                'result': result,
                'success': True
            })
            
            logger.info(f"✅ Successfully processed {file}")
            logger.info(f"📊 Matches found: {result['data_volumes']['total_matches']}")
            
        except Exception as e:
            logger.error(f"❌ Error processing {file}: {e}")
            all_results.append({
                'file': file,
                'error': str(e),
                'success': False
            })
    
    # Generate summary report
    logger.info("\n📊 Pipeline Summary:")
    logger.info("=" * 50)
    
    successful_files = [r for r in all_results if r['success']]
    failed_files = [r for r in all_results if not r['success']]
    
    logger.info(f"✅ Successful: {len(successful_files)}/{len(normalized_files)} files")
    logger.info(f"❌ Failed: {len(failed_files)}/{len(normalized_files)} files")
    
    if failed_files:
        logger.info("\n❌ Failed files:")
        for result in failed_files:
            logger.info(f"  - {result['file']}: {result['error']}")
    
    if successful_files:
        logger.info("\n✅ Successful files:")
        total_matches = 0
        for result in successful_files:
            matches = result['result']['data_volumes']['total_matches']
            total_matches += matches
            logger.info(f"  - {result['file']}: {matches} matches")
        
        logger.info(f"\n📊 Total matches across all files: {total_matches}")
    
    logger.info(f"\n📁 Results saved to: {output_dir}")
    
    return len(failed_files) == 0

def main():
    """Main execution function."""
    
    logger.info("🎯 Pipeline with Normalized Dates")
    logger.info("=" * 50)
    
    success = run_pipeline_with_normalized_dates()
    
    if success:
        logger.info("\n🎉 Pipeline completed successfully!")
    else:
        logger.error("\n❌ Pipeline completed with errors")
    
    return success

if __name__ == "__main__":
    main() 
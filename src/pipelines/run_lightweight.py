#!/usr/bin/env python3
"""
Lightweight Pipeline Runner
Runs the matching pipeline with minimal resource usage.
Perfect for systems with limited available resources.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
import psutil
from pathlib import Path

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from matching.lightweight_pipeline import run_lightweight_pipeline

def setup_logging():
    """Setup logging for the lightweight pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('lightweight_pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def check_system_resources():
    """Check if system has enough resources to run."""
    memory_gb = psutil.virtual_memory().available / (1024**3)
    cpu_percent = psutil.cpu_percent()
    
    print(f"System Resources:")
    print(f"  Available Memory: {memory_gb:.1f} GB")
    print(f"  Current CPU Usage: {cpu_percent:.1f}%")
    
    if memory_gb < 1:
        print("⚠️  WARNING: Very low memory available (< 1GB)")
        print("   The pipeline may run slowly or fail")
        return False
    elif memory_gb < 2:
        print("⚠️  WARNING: Low memory available (< 2GB)")
        print("   The pipeline will use minimal settings")
        return True
    else:
        print("✅ Sufficient resources available")
        return True

def main():
    parser = argparse.ArgumentParser(description='Run lightweight CSV matching pipeline')
    parser.add_argument('--owners', required=True, help='Path to owners CSV file')
    parser.add_argument('--transactions', required=True, help='Path to transactions CSV file')
    parser.add_argument('--output', default='data/processed/lightweight', help='Output directory')
    parser.add_argument('--run-id', help='Custom run ID (optional)')
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("🚀 Lightweight CSV Matching Pipeline")
    print("=" * 50)
    
    # Check system resources
    if not check_system_resources():
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            print("Pipeline cancelled")
            return
    
    # Validate input files
    if not os.path.exists(args.owners):
        print(f"❌ Owners file not found: {args.owners}")
        return
    
    if not os.path.exists(args.transactions):
        print(f"❌ Transactions file not found: {args.transactions}")
        return
    
    print(f"\n📁 Input Files:")
    print(f"  Owners: {args.owners}")
    print(f"  Transactions: {args.transactions}")
    print(f"  Output: {args.output}")
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Generate run ID if not provided
    if not args.run_id:
        args.run_id = f"lightweight_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n🆔 Run ID: {args.run_id}")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "=" * 50)
    
    try:
        # Run the lightweight pipeline
        start_time = datetime.now()
        
        stats = run_lightweight_pipeline(
            owners_file=args.owners,
            transactions_file=args.transactions,
            output_dir=args.output,
            run_id=args.run_id
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print results
        print("\n" + "=" * 50)
        print("✅ Lightweight Pipeline Completed!")
        print(f"⏱️  Duration: {duration}")
        print(f"📊 Results:")
        print(f"  Total Owners: {stats['data_volumes']['total_owners']:,}")
        print(f"  Total Transactions: {stats['data_volumes']['total_transactions']:,}")
        print(f"  Total Matches: {stats['data_volumes']['total_matches']:,}")
        print(f"  Owner Match Rate: {stats['match_rates']['owner_match_rate']:.1%}")
        print(f"  Transaction Match Rate: {stats['match_rates']['transaction_match_rate']:.1%}")
        print(f"  Tier 1 Matches: {stats['tier_performance']['tier1_matches']:,}")
        print(f"  Tier 2 Matches: {stats['tier_performance']['tier2_matches']:,}")
        
        print(f"\n💾 Output Files:")
        print(f"  Matches: {os.path.join(args.output, 'lightweight_matches.parquet')}")
        print(f"  Log: lightweight_pipeline.log")
        
        # Save statistics
        import json
        stats_file = os.path.join(args.output, f"{args.run_id}_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        print(f"  Statistics: {stats_file}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        print(f"\n❌ Pipeline failed: {str(e)}")
        print("Check lightweight_pipeline.log for details")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 
#!/usr/bin/env python3
"""
Resource-Friendly Pipeline Runner
Uses only available system resources without overwhelming the system.
Perfect for running alongside other applications.
"""

import os
import sys
import logging
import argparse
from datetime import datetime
import psutil
import multiprocessing as mp
from pathlib import Path

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

def calculate_optimal_resources():
    """Calculate optimal resource usage based on available system resources."""
    # Get system info
    memory_gb = psutil.virtual_memory().available / (1024**3)
    cpu_percent = psutil.cpu_percent()
    cpu_count = psutil.cpu_count()
    
    # Calculate safe resource usage (leave 30% for other applications)
    safe_memory_gb = memory_gb * 0.7
    safe_cpu_cores = max(1, int(cpu_count * 0.5))  # Use 50% of cores max
    
    # Adjust based on current CPU usage
    if cpu_percent > 50:
        safe_cpu_cores = max(1, safe_cpu_cores // 2)  # Use fewer cores if CPU is busy
        safe_memory_gb *= 0.8  # Use less memory if CPU is busy
    
    # Ensure minimum viable resources
    safe_cpu_cores = max(2, min(safe_cpu_cores, 8))  # Between 2-8 cores
    safe_memory_gb = max(2, min(safe_memory_gb, 8))  # Between 2-8GB
    
    return {
        'cores': safe_cpu_cores,
        'memory_gb': safe_memory_gb,
        'chunk_size': int(5000 * (safe_memory_gb / 4)),  # Scale chunk size with memory
        'batch_size': int(200 * (safe_memory_gb / 4))    # Scale batch size with memory
    }

def run_resource_friendly_pipeline(owners_file, transactions_file, output_dir, run_id=None):
    """Run the pipeline with resource-friendly settings."""
    if run_id is None:
        run_id = f"resource_friendly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    # Calculate optimal resources
    resources = calculate_optimal_resources()
    
    print(f"🔧 Resource-Friendly Settings:")
    print(f"   Cores: {resources['cores']}/{psutil.cpu_count()}")
    print(f"   Memory: {resources['memory_gb']:.1f}GB")
    print(f"   Chunk Size: {resources['chunk_size']:,}")
    print(f"   Batch Size: {resources['batch_size']:,}")
    
    # Import and run the appropriate pipeline
    if resources['cores'] >= 6 and resources['memory_gb'] >= 6:
        # Use ultra-fast pipeline with reduced settings
        from matching.m4_ultra_pipeline import run_m4_ultra_pipeline
        print("🚀 Using M4 Ultra Pipeline (reduced settings)")
        return run_m4_ultra_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir=output_dir,
            run_id=run_id
        )
    elif resources['cores'] >= 4 and resources['memory_gb'] >= 4:
        # Use fast pipeline
        from matching.fast_pipeline import run_fast_pipeline
        print("⚡ Using Fast Pipeline")
        return run_fast_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir=output_dir,
            run_id=run_id,
            max_workers=resources['cores']
        )
    else:
        # Use lightweight pipeline
        from matching.lightweight_pipeline import run_lightweight_pipeline
        print("🪶 Using Lightweight Pipeline")
        return run_lightweight_pipeline(
            owners_file=owners_file,
            transactions_file=transactions_file,
            output_dir=output_dir,
            run_id=run_id
        )

def main():
    parser = argparse.ArgumentParser(description='Run resource-friendly CSV matching pipeline')
    parser.add_argument('--owners', required=True, help='Path to owners CSV file')
    parser.add_argument('--transactions', required=True, help='Path to transactions CSV file')
    parser.add_argument('--output', default='data/processed/resource_friendly', help='Output directory')
    parser.add_argument('--run-id', help='Custom run ID (optional)')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('resource_friendly_pipeline.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    print("🚀 Resource-Friendly CSV Matching Pipeline")
    print("=" * 60)
    
    # Check system resources
    memory_gb = psutil.virtual_memory().available / (1024**3)
    cpu_percent = psutil.cpu_percent()
    cpu_count = psutil.cpu_count()
    
    print(f"📊 Current System Status:")
    print(f"   Available Memory: {memory_gb:.1f} GB")
    print(f"   CPU Usage: {cpu_percent:.1f}%")
    print(f"   Total Cores: {cpu_count}")
    
    # Validate input files
    if not os.path.exists(args.owners):
        print(f"❌ Owners file not found: {args.owners}")
        return 1
    
    if not os.path.exists(args.transactions):
        print(f"❌ Transactions file not found: {args.transactions}")
        return 1
    
    print(f"\n📁 Input Files:")
    print(f"  Owners: {args.owners}")
    print(f"  Transactions: {args.transactions}")
    print(f"  Output: {args.output}")
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Generate run ID if not provided
    if not args.run_id:
        args.run_id = f"resource_friendly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    print(f"\n🆔 Run ID: {args.run_id}")
    print(f"⏰ Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n" + "=" * 60)
    
    try:
        # Run the resource-friendly pipeline
        start_time = datetime.now()
        
        stats = run_resource_friendly_pipeline(
            owners_file=args.owners,
            transactions_file=args.transactions,
            output_dir=args.output,
            run_id=args.run_id
        )
        
        end_time = datetime.now()
        duration = end_time - start_time
        
        # Print results
        print("\n" + "=" * 60)
        print("✅ Resource-Friendly Pipeline Completed!")
        print(f"⏱️  Duration: {duration}")
        print(f"📊 Results:")
        print(f"  Total Owners: {stats['data_volumes']['total_owners']:,}")
        print(f"  Total Transactions: {stats['data_volumes']['total_transactions']:,}")
        print(f"  Total Matches: {stats['data_volumes']['total_matches']:,}")
        print(f"  Owner Match Rate: {stats['match_rates']['owner_match_rate']:.1%}")
        print(f"  Transaction Match Rate: {stats['match_rates']['transaction_match_rate']:.1%}")
        
        print(f"\n💾 Output Files:")
        print(f"  Log: resource_friendly_pipeline.log")
        
        # Save statistics
        import json
        stats_file = os.path.join(args.output, f"{args.run_id}_stats.json")
        with open(stats_file, 'w') as f:
            json.dump(stats, f, indent=2, default=str)
        print(f"  Statistics: {stats_file}")
        
    except Exception as e:
        logging.error(f"Pipeline failed: {str(e)}")
        print(f"\n❌ Pipeline failed: {str(e)}")
        print("Check resource_friendly_pipeline.log for details")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main()) 
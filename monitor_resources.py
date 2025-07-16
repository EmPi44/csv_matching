#!/usr/bin/env python3
"""
Resource Monitor
Monitors system resources and provides recommendations.
"""

import psutil
import time
import os
from datetime import datetime

def get_system_info():
    """Get current system resource information."""
    # CPU info
    cpu_percent = psutil.cpu_percent(interval=1)
    cpu_count = psutil.cpu_count()
    
    # Memory info
    memory = psutil.virtual_memory()
    memory_total_gb = memory.total / (1024**3)
    memory_used_gb = memory.used / (1024**3)
    memory_available_gb = memory.available / (1024**3)
    memory_percent = memory.percent
    
    # Disk info
    disk = psutil.disk_usage('/')
    disk_total_gb = disk.total / (1024**3)
    disk_used_gb = disk.used / (1024**3)
    disk_free_gb = disk.free / (1024**3)
    disk_percent = (disk.used / disk.total) * 100
    
    return {
        'cpu_percent': cpu_percent,
        'cpu_count': cpu_count,
        'memory_total_gb': memory_total_gb,
        'memory_used_gb': memory_used_gb,
        'memory_available_gb': memory_available_gb,
        'memory_percent': memory_percent,
        'disk_total_gb': disk_total_gb,
        'disk_used_gb': disk_used_gb,
        'disk_free_gb': disk_free_gb,
        'disk_percent': disk_percent
    }

def get_recommendations(info):
    """Get recommendations based on current resource usage."""
    recommendations = []
    
    # Memory recommendations
    if info['memory_available_gb'] < 1:
        recommendations.append("🚨 CRITICAL: Less than 1GB RAM available")
        recommendations.append("   - Close unnecessary applications")
        recommendations.append("   - Use lightweight pipeline only")
    elif info['memory_available_gb'] < 2:
        recommendations.append("⚠️  WARNING: Less than 2GB RAM available")
        recommendations.append("   - Consider closing some applications")
        recommendations.append("   - Use lightweight pipeline")
    elif info['memory_available_gb'] < 4:
        recommendations.append("ℹ️  INFO: Moderate memory available")
        recommendations.append("   - Can run lightweight pipeline")
        recommendations.append("   - Consider closing heavy applications for better performance")
    else:
        recommendations.append("✅ Good memory availability")
        recommendations.append("   - Can run any pipeline version")
    
    # CPU recommendations
    if info['cpu_percent'] > 80:
        recommendations.append("🚨 CRITICAL: High CPU usage (>80%)")
        recommendations.append("   - Close CPU-intensive applications")
        recommendations.append("   - Wait for system to cool down")
    elif info['cpu_percent'] > 60:
        recommendations.append("⚠️  WARNING: Elevated CPU usage (>60%)")
        recommendations.append("   - Consider closing some applications")
        recommendations.append("   - Pipeline may run slower")
    else:
        recommendations.append("✅ Good CPU availability")
    
    # Disk recommendations
    if info['disk_free_gb'] < 5:
        recommendations.append("🚨 CRITICAL: Low disk space (<5GB)")
        recommendations.append("   - Clean up temporary files")
        recommendations.append("   - Clear browser cache")
    elif info['disk_free_gb'] < 10:
        recommendations.append("⚠️  WARNING: Limited disk space (<10GB)")
        recommendations.append("   - Consider cleaning up files")
    
    return recommendations

def print_status(info, recommendations):
    """Print formatted status information."""
    print("\n" + "="*60)
    print(f"🖥️  SYSTEM RESOURCES - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # CPU
    print(f"💻 CPU:")
    print(f"   Usage: {info['cpu_percent']:.1f}%")
    print(f"   Cores: {info['cpu_count']}")
    
    # Memory
    print(f"\n🧠 MEMORY:")
    print(f"   Total: {info['memory_total_gb']:.1f} GB")
    print(f"   Used: {info['memory_used_gb']:.1f} GB ({info['memory_percent']:.1f}%)")
    print(f"   Available: {info['memory_available_gb']:.1f} GB")
    
    # Disk
    print(f"\n💾 DISK:")
    print(f"   Total: {info['disk_total_gb']:.1f} GB")
    print(f"   Used: {info['disk_used_gb']:.1f} GB ({info['disk_percent']:.1f}%)")
    print(f"   Free: {info['disk_free_gb']:.1f} GB")
    
    # Recommendations
    print(f"\n💡 RECOMMENDATIONS:")
    for rec in recommendations:
        print(f"   {rec}")
    
    print("="*60)

def monitor_continuously(interval=30):
    """Monitor resources continuously."""
    print("🔍 Starting continuous resource monitoring...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            info = get_system_info()
            recommendations = get_recommendations(info)
            print_status(info, recommendations)
            
            print(f"\n⏰ Next update in {interval} seconds...")
            time.sleep(interval)
            
    except KeyboardInterrupt:
        print("\n🛑 Monitoring stopped")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Monitor system resources')
    parser.add_argument('--continuous', '-c', action='store_true', 
                       help='Monitor continuously')
    parser.add_argument('--interval', '-i', type=int, default=30,
                       help='Update interval in seconds (default: 30)')
    
    args = parser.parse_args()
    
    if args.continuous:
        monitor_continuously(args.interval)
    else:
        info = get_system_info()
        recommendations = get_recommendations(info)
        print_status(info, recommendations)

if __name__ == "__main__":
    main() 
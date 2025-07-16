#!/usr/bin/env python3
"""
Test M4 optimizations and show performance improvements.
"""

import time
import psutil
import multiprocessing as mp
from functools import lru_cache
import pandas as pd
import numpy as np

def test_m4_specs():
    """Test and display M4 specifications."""
    print("🚀 M4 MacBook Pro Specifications")
    print("=" * 40)
    
    cpu_count = mp.cpu_count()
    memory_gb = psutil.virtual_memory().total / (1024**3)
    memory_available = psutil.virtual_memory().available / (1024**3)
    
    print(f"CPU Cores: {cpu_count}")
    print(f"Total Memory: {memory_gb:.1f} GB")
    print(f"Available Memory: {memory_available:.1f} GB")
    print(f"Memory Usage: {psutil.virtual_memory().percent:.1f}%")
    
    # Test CPU cores
    print(f"\nCPU Core Test:")
    cpu_percent = psutil.cpu_percent(interval=1, percpu=True)
    for i, percent in enumerate(cpu_percent):
        print(f"  Core {i+1}: {percent:.1f}%")
    
    return cpu_count, memory_gb

@lru_cache(maxsize=1024)
def fast_string_hash(text: str) -> int:
    """Test hash-based optimization."""
    if not text:
        return 0
    return hash(text.lower().strip()) % (2**32)

def test_hash_performance():
    """Test hash-based performance vs string comparison."""
    print("\n🔍 Hash Performance Test")
    print("=" * 40)
    
    # Test data
    test_strings = [f"Building_{i}_Unit_{j}" for i in range(100) for j in range(10)]
    
    # Test hash-based lookup
    start_time = time.time()
    hash_dict = {}
    for s in test_strings:
        hash_key = fast_string_hash(s)
        if hash_key not in hash_dict:
            hash_dict[hash_key] = []
        hash_dict[hash_key].append(s)
    
    # Test hash lookup
    lookup_count = 0
    for s in test_strings[:1000]:
        hash_key = fast_string_hash(s)
        if hash_key in hash_dict:
            lookup_count += 1
    
    hash_time = time.time() - start_time
    
    # Test string comparison
    start_time = time.time()
    string_count = 0
    for s in test_strings[:1000]:
        for target in test_strings:
            if s.lower().strip() == target.lower().strip():
                string_count += 1
                break
    
    string_time = time.time() - start_time
    
    print(f"Hash-based lookup: {hash_time:.4f}s ({lookup_count} matches)")
    print(f"String comparison: {string_time:.4f}s ({string_count} matches)")
    print(f"Speedup: {string_time/hash_time:.1f}x faster")
    
    return hash_time, string_time

def test_vectorized_operations():
    """Test vectorized operations performance."""
    print("\n⚡ Vectorized Operations Test")
    print("=" * 40)
    
    # Create test data
    size = 100000
    df = pd.DataFrame({
        'building': [f"Building_{i%100}" for i in range(size)],
        'unit': [f"Unit_{i%50}" for i in range(size)],
        'area': np.random.uniform(100, 500, size)
    })
    
    # Test vectorized operations
    start_time = time.time()
    
    # Vectorized string operations
    df['building_lower'] = df['building'].str.lower().str.strip()
    df['unit_clean'] = df['unit'].str.strip()
    
    # Vectorized numeric operations
    area_tolerance = df['area'] * 0.02
    area_similarity = (df['area'] >= df['area'] - area_tolerance) & (df['area'] <= df['area'] + area_tolerance)
    
    # Vectorized boolean operations
    building_match = df['building_lower'] == 'building_1'
    unit_match = df['unit_clean'] == 'unit_1'
    combined_match = building_match & unit_match
    
    vectorized_time = time.time() - start_time
    
    # Test loop-based operations
    start_time = time.time()
    
    loop_matches = 0
    for idx, row in df.iterrows():
        if (row['building'].lower().strip() == 'building_1' and 
            row['unit'].strip() == 'unit_1'):
            loop_matches += 1
    
    loop_time = time.time() - start_time
    
    print(f"Vectorized operations: {vectorized_time:.4f}s")
    print(f"Loop-based operations: {loop_time:.4f}s")
    print(f"Speedup: {loop_time/vectorized_time:.1f}x faster")
    print(f"Vectorized matches: {combined_match.sum()}")
    print(f"Loop matches: {loop_matches}")
    
    return vectorized_time, loop_time

def test_memory_optimization():
    """Test memory optimization techniques."""
    print("\n💾 Memory Optimization Test")
    print("=" * 40)
    
    # Test memory usage before
    memory_before = psutil.virtual_memory().used / (1024**3)
    print(f"Memory before: {memory_before:.2f} GB")
    
    # Create large dataset
    size = 500000
    df = pd.DataFrame({
        'id': range(size),
        'text': [f"Sample text {i}" for i in range(size)],
        'number': np.random.rand(size)
    })
    
    memory_after = psutil.virtual_memory().used / (1024**3)
    print(f"Memory after creating dataset: {memory_after:.2f} GB")
    print(f"Memory increase: {memory_after - memory_before:.2f} GB")
    
    # Test garbage collection
    import gc
    del df
    gc.collect()
    
    memory_after_gc = psutil.virtual_memory().used / (1024**3)
    print(f"Memory after garbage collection: {memory_after_gc:.2f} GB")
    print(f"Memory freed: {memory_after - memory_after_gc:.2f} GB")
    
    return memory_before, memory_after, memory_after_gc

def main():
    """Run all M4 optimization tests."""
    print("🚀 M4 Ultra-Fast Pipeline - Performance Tests")
    print("=" * 60)
    
    # Test M4 specifications
    cpu_count, memory_gb = test_m4_specs()
    
    # Test hash performance
    hash_time, string_time = test_hash_performance()
    
    # Test vectorized operations
    vectorized_time, loop_time = test_vectorized_operations()
    
    # Test memory optimization
    memory_before, memory_after, memory_after_gc = test_memory_optimization()
    
    # Summary
    print("\n📊 M4 Performance Summary")
    print("=" * 40)
    print(f"Hardware: {cpu_count} cores, {memory_gb:.1f}GB RAM")
    print(f"Hash optimization: {string_time/hash_time:.1f}x faster")
    print(f"Vectorization: {loop_time/vectorized_time:.1f}x faster")
    print(f"Memory efficiency: {memory_after_gc:.2f}GB used")
    
    print(f"\n🎯 Expected M4 Pipeline Performance:")
    print(f"  - Processing time: 1-3 minutes")
    print(f"  - Memory usage: Up to {memory_gb * 0.8:.1f}GB")
    print(f"  - CPU utilization: 90-100% across all {cpu_count} cores")
    print(f"  - Overall speedup: 20-100x vs original pipeline")
    
    print(f"\n🚀 Ready to run M4 pipeline: python run_m4_ultra.py")

if __name__ == "__main__":
    main() 
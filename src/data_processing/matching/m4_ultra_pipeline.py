"""
M4 Ultra-Fast Pipeline - Optimized for Apple Silicon M4
Advanced performance techniques for maximum speed.
"""

import os
import pandas as pd
import numpy as np
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any
import gc
import psutil
from functools import lru_cache
import hashlib
import pickle
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# M4-specific optimizations
M4_CORES = 14  # M4 has 14 cores
M4_MEMORY_GB = 24  # 24GB RAM
OPTIMAL_CHUNK_SIZE = 10000  # Smaller chunks for better cache utilization
OPTIMAL_BATCH_SIZE = 250  # Smaller batches for M4 efficiency

# Memory management
MAX_MEMORY_USAGE = 0.8  # Use up to 80% of available RAM
CACHE_SIZE = 1024  # LRU cache size for repeated operations

def get_memory_usage():
    """Get current memory usage percentage."""
    return psutil.virtual_memory().percent / 100

def optimize_for_m4():
    """Apply M4-specific optimizations."""
    # Optimize numpy for M4
    np.set_printoptions(precision=3, suppress=True)
    
    # Set multiprocessing start method for M4
    if mp.get_start_method() != 'spawn':
        mp.set_start_method('spawn', force=True)
    
    logger.info(f"M4 optimizations applied: {M4_CORES} cores, {M4_MEMORY_GB}GB RAM")

@lru_cache(maxsize=CACHE_SIZE)
def fast_string_hash(text: str) -> int:
    """Ultra-fast string hashing for M4."""
    if not text:
        return 0
    return hash(text.lower().strip()) % (2**32)

def create_m4_index(owners_df: pd.DataFrame, transactions_df: pd.DataFrame) -> Dict:
    """
    Create optimized indexes for M4 processing.
    Uses hash-based indexing for maximum speed.
    """
    logger.info("Creating M4-optimized indexes")
    
    # Create hash-based indexes
    owner_hashes = {}
    transaction_hashes = {}
    
    # Index owners by building + unit combination
    for idx, row in owners_df.iterrows():
        building = str(row.get('building_clean', '')).lower().strip()
        unit = str(row.get('unit_no', '')).strip()
        area = row.get('area_sqm', 0)
        
        # Create composite hash
        composite = f"{building}|{unit}|{area}"
        hash_key = fast_string_hash(composite)
        
        if hash_key not in owner_hashes:
            owner_hashes[hash_key] = []
        owner_hashes[hash_key].append(idx)
    
    # Index transactions similarly
    for idx, row in transactions_df.iterrows():
        building = str(row.get('building_clean', '')).lower().strip()
        unit = str(row.get('unit_no', '')).strip()
        area = row.get('area_sqm', 0)
        
        composite = f"{building}|{unit}|{area}"
        hash_key = fast_string_hash(composite)
        
        if hash_key not in transaction_hashes:
            transaction_hashes[hash_key] = []
        transaction_hashes[hash_key].append(idx)
    
    logger.info(f"Created indexes: {len(owner_hashes)} owner groups, {len(transaction_hashes)} transaction groups")
    
    return {
        'owner_hashes': owner_hashes,
        'transaction_hashes': transaction_hashes
    }

def m4_deterministic_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    indexes: Dict
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    M4-optimized deterministic matching using hash-based indexes.
    """
    logger.info("Starting M4 deterministic matching")
    
    matches = []
    matched_owner_ids = set()
    matched_txn_ids = set()
    
    # Use hash-based matching for maximum speed
    owner_hashes = indexes['owner_hashes']
    transaction_hashes = indexes['transaction_hashes']
    
    # Find exact hash matches
    for hash_key, owner_indices in owner_hashes.items():
        if hash_key in transaction_hashes:
            txn_indices = transaction_hashes[hash_key]
            
            # Create all combinations
            for owner_idx in owner_indices:
                for txn_idx in txn_indices:
                    owner_row = owners_df.iloc[owner_idx]
                    txn_row = transactions_df.iloc[txn_idx]
                    
                    # Double-check match (hash collision protection)
                    if (str(owner_row.get('building_clean', '')).lower().strip() == 
                        str(txn_row.get('building_clean', '')).lower().strip() and
                        str(owner_row.get('unit_no', '')).strip() == 
                        str(txn_row.get('unit_no', '')).strip()):
                        
                        matches.append({
                            'owner_id': owner_row['owner_id'],
                            'txn_id': txn_row['txn_id'],
                            'match_type': 'deterministic',
                            'confidence': 1.0,
                            'confidence_bucket': 'High',
                            'match_score': 1.0
                        })
                        
                        matched_owner_ids.add(owner_row['owner_id'])
                        matched_txn_ids.add(txn_row['txn_id'])
    
    matches_df = pd.DataFrame(matches)
    
    # Get unmatched records
    unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"M4 deterministic matching: {len(matches_df)} exact matches")
    return matches_df, unmatched_owners, unmatched_transactions

def m4_fuzzy_match_batch(
    owners_batch: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75
) -> List[Dict]:
    """
    M4-optimized fuzzy matching for a batch of owners.
    Uses vectorized operations and M4-specific optimizations.
    """
    matches = []
    
    # Pre-compute transaction building names for vectorized operations
    txn_buildings = transactions_df['building_clean'].astype(str).str.lower().str.strip()
    txn_units = transactions_df['unit_no'].astype(str).str.strip()
    txn_areas = transactions_df['area_sqm'].fillna(0)
    
    for _, owner_row in owners_batch.iterrows():
        owner_building = str(owner_row['building_clean']).lower().strip()
        owner_unit = str(owner_row['unit_no']).strip()
        owner_area = owner_row['area_sqm'] if pd.notna(owner_row['area_sqm']) else 0
        
        # Vectorized building similarity check
        building_similarity = (
            (txn_buildings == owner_building) |
            (txn_buildings.str.contains(owner_building, na=False)) |
            (owner_building in txn_buildings.values)
        )
        
        # Vectorized unit similarity
        unit_similarity = (txn_units == owner_unit)
        
        # Vectorized area similarity (within 2% tolerance)
        if owner_area > 0:
            area_tolerance = owner_area * 0.02
            area_similarity = (txn_areas >= owner_area - area_tolerance) & (txn_areas <= owner_area + area_tolerance)
        else:
            area_similarity = pd.Series([True] * len(transactions_df), index=transactions_df.index)
        
        # Combined similarity mask
        similarity_mask = building_similarity & (unit_similarity | area_similarity)
        candidate_indices = similarity_mask[similarity_mask].index
        
        # Calculate scores for candidates
        for idx in candidate_indices[:10]:  # Limit to top 10 candidates
            txn_row = transactions_df.loc[idx]
            
            # Fast score calculation
            building_score = 0.9 if txn_buildings[idx] == owner_building else 0.7
            unit_score = 1.0 if txn_units[idx] == owner_unit else 0.0
            
            # Area score
            if owner_area > 0 and txn_areas[idx] > 0:
                area_diff_pct = abs(owner_area - txn_areas[idx]) / owner_area
                area_score = max(0.0, 1.0 - (area_diff_pct / 0.02))
            else:
                area_score = 0.0
            
            total_score = (0.6 * building_score + 0.3 * unit_score + 0.1 * area_score)
            
            if total_score >= min_score:
                matches.append({
                    'owner_id': owner_row['owner_id'],
                    'txn_id': txn_row['txn_id'],
                    'match_type': 'fuzzy',
                    'confidence': total_score,
                    'confidence_bucket': 'High' if total_score >= 0.9 else 'Medium',
                    'match_score': total_score
                })
    
    return matches

def m4_fuzzy_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    chunk_size: int = OPTIMAL_BATCH_SIZE
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    M4-optimized fuzzy matching with aggressive parallelization.
    """
    logger.info("Starting M4 fuzzy matching")
    
    if len(owners_df) == 0 or len(transactions_df) == 0:
        return pd.DataFrame(), owners_df, transactions_df
    
    # Split owners into optimal chunks for M4
    owner_chunks = [owners_df[i:i+chunk_size] for i in range(0, len(owners_df), chunk_size)]
    
    logger.info(f"Processing {len(owner_chunks)} chunks with {M4_CORES} M4 cores")
    
    # Process chunks in parallel with M4 optimization
    all_matches = []
    
    with ProcessPoolExecutor(max_workers=M4_CORES) as executor:
        # Submit all chunks
        future_to_chunk = {}
        for i, chunk in enumerate(owner_chunks):
            future = executor.submit(
                m4_fuzzy_match_batch,
                chunk, transactions_df, min_score
            )
            future_to_chunk[future] = i
        
        # Collect results with memory management
        for future in as_completed(future_to_chunk):
            chunk_idx = future_to_chunk[future]
            try:
                chunk_matches = future.result()
                all_matches.extend(chunk_matches)
                
                # Memory management
                if get_memory_usage() > MAX_MEMORY_USAGE:
                    gc.collect()
                
                logger.info(f"M4 completed chunk {chunk_idx + 1}/{len(owner_chunks)}")
            except Exception as e:
                logger.error(f"Error in M4 chunk {chunk_idx}: {e}")
    
    # Create matches DataFrame
    if all_matches:
        matches_df = pd.DataFrame(all_matches)
        
        # Remove duplicates (keep highest score)
        matches_df = matches_df.sort_values('match_score', ascending=False)
        matches_df = matches_df.drop_duplicates(subset=['owner_id', 'txn_id'], keep='first')
        
        logger.info(f"M4 fuzzy matching: {len(matches_df)} matches")
    else:
        matches_df = pd.DataFrame()
        logger.info("M4 fuzzy matching: No matches found")
    
    # Get unmatched records
    matched_owner_ids = set(matches_df['owner_id'].unique()) if len(matches_df) > 0 else set()
    matched_txn_ids = set(matches_df['txn_id'].unique()) if len(matches_df) > 0 else set()
    
    unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    
    return matches_df, unmatched_owners, unmatched_transactions

def run_m4_ultra_pipeline(
    owners_file: str,
    transactions_file: str,
    output_dir: str = "data/processed",
    run_id: str = None
) -> Dict[str, Any]:
    """
    Run the M4-optimized ultra-fast matching pipeline.
    """
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"Starting M4 ultra-fast pipeline: {run_id}")
    
    # Apply M4 optimizations
    optimize_for_m4()
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data with M4 optimizations
    logger.info("Loading data with M4 optimizations")
    from .preprocess import preprocess_owners, preprocess_transactions
    
    # Load data
    owners_df = pd.read_csv(owners_file, low_memory=False) if owners_file.endswith('.csv') else pd.read_excel(owners_file)
    transactions_df = pd.read_csv(transactions_file, low_memory=False) if transactions_file.endswith('.csv') else pd.read_excel(transactions_file)
    
    # Preprocess data
    owners_clean = preprocess_owners(owners_df)
    transactions_clean = preprocess_transactions(transactions_df)
    
    # Create M4-optimized indexes
    indexes = create_m4_index(owners_clean, transactions_clean)
    
    # M4 deterministic matching
    logger.info("Running M4 deterministic matching")
    tier1_matches, unmatched_owners, unmatched_transactions = m4_deterministic_match(
        owners_clean, transactions_clean, indexes
    )
    
    # M4 fuzzy matching
    logger.info("Running M4 fuzzy matching")
    tier2_matches, final_unmatched_owners, final_unmatched_transactions = m4_fuzzy_match(
        unmatched_owners, unmatched_transactions
    )
    
    # Combine all matches
    all_matches = pd.concat([tier1_matches, tier2_matches], ignore_index=True)
    
    # Save results with M4 optimizations
    logger.info("Saving M4 pipeline results")
    matches_path = os.path.join(output_dir, "m4_ultra_matches.parquet")
    all_matches.to_parquet(matches_path, index=False, compression='snappy')
    
    # Save intermediate results
    if len(tier1_matches) > 0:
        tier1_path = os.path.join(output_dir, "m4_tier1_matches.parquet")
        tier1_matches.to_parquet(tier1_path, index=False, compression='snappy')
    
    if len(tier2_matches) > 0:
        tier2_path = os.path.join(output_dir, "m4_tier2_matches.parquet")
        tier2_matches.to_parquet(tier2_path, index=False, compression='snappy')
    
    # Save unmatched records
    if len(final_unmatched_owners) > 0:
        unmatched_owners_path = os.path.join(output_dir, "m4_unmatched_owners.csv")
        final_unmatched_owners.to_csv(unmatched_owners_path, index=False)
    
    if len(final_unmatched_transactions) > 0:
        unmatched_txns_path = os.path.join(output_dir, "m4_unmatched_transactions.csv")
        final_unmatched_transactions.to_csv(unmatched_txns_path, index=False)
    
    # Compile M4 statistics
    total_owners = len(owners_clean)
    total_transactions = len(transactions_clean)
    total_matches = len(all_matches)
    tier1_count = len(tier1_matches)
    tier2_count = len(tier2_matches)
    
    stats = {
        'run_id': run_id,
        'timestamp': datetime.now().isoformat(),
        'platform': 'M4 MacBook Pro',
        'cores_used': M4_CORES,
        'memory_gb': M4_MEMORY_GB,
        'data_volumes': {
            'total_owners': total_owners,
            'total_transactions': total_transactions,
            'total_matches': total_matches
        },
        'match_rates': {
            'owner_match_rate': total_matches / total_owners if total_owners > 0 else 0,
            'transaction_match_rate': total_matches / total_transactions if total_transactions > 0 else 0
        },
        'tier_performance': {
            'tier1_matches': tier1_count,
            'tier2_matches': tier2_count,
            'tier1_rate': tier1_count / total_owners if total_owners > 0 else 0,
            'tier2_rate': tier2_count / total_owners if total_owners > 0 else 0
        }
    }
    
    logger.info(f"M4 pipeline completed: {total_matches} matches in {total_owners} owners and {total_transactions} transactions")
    return stats 
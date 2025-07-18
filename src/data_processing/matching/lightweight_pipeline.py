"""
Lightweight Pipeline - Minimal Resource Usage
Optimized for systems with limited available resources.
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any
import gc
import psutil
from pathlib import Path

# Configure logging
logger = logging.getLogger(__name__)

# Lightweight settings - minimal resource usage
LIGHTWEIGHT_CORES = 4  # Use only 4 cores to leave resources for other apps
LIGHTWEIGHT_MEMORY_GB = 4  # Use only 4GB RAM max
LIGHTWEIGHT_CHUNK_SIZE = 1000  # Small chunks for low memory usage
LIGHTWEIGHT_BATCH_SIZE = 100  # Small batches

def get_available_resources():
    """Check available system resources."""
    memory_gb = psutil.virtual_memory().available / (1024**3)
    cpu_percent = psutil.cpu_percent()
    
    logger.info(f"Available resources: {memory_gb:.1f}GB RAM, {cpu_percent:.1f}% CPU")
    
    # Adjust settings based on available resources
    if memory_gb < 2:
        logger.warning("Low memory available, using minimal settings")
        return 2, 2  # 2 cores, 2GB RAM
    elif memory_gb < 4:
        logger.info("Moderate memory available, using conservative settings")
        return 3, 3  # 3 cores, 3GB RAM
    else:
        logger.info("Sufficient resources available")
        return LIGHTWEIGHT_CORES, LIGHTWEIGHT_MEMORY_GB

def lightweight_deterministic_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lightweight deterministic matching with minimal memory usage.
    """
    logger.info("Starting lightweight deterministic matching")
    
    matches = []
    matched_owner_ids = set()
    matched_txn_ids = set()
    
    # Process in small batches to minimize memory usage
    batch_size = LIGHTWEIGHT_BATCH_SIZE
    
    for i in range(0, len(owners_df), batch_size):
        owner_batch = owners_df.iloc[i:i+batch_size]
        
        for _, owner_row in owner_batch.iterrows():
            owner_building = str(owner_row.get('building_clean', '')).lower().strip()
            owner_unit = str(owner_row.get('unit_no', '')).strip()
            owner_area = owner_row.get('area_sqm', 0)
            
            # Find exact matches in transactions
            for _, txn_row in transactions_df.iterrows():
                txn_building = str(txn_row.get('building_clean', '')).lower().strip()
                txn_unit = str(txn_row.get('unit_no', '')).strip()
                txn_area = txn_row.get('area_sqm', 0)
                
                # Exact match check
                if (owner_building == txn_building and 
                    owner_unit == txn_unit and
                    abs(owner_area - txn_area) < 1):  # Within 1 sqm
                    
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
                    break  # Found match, move to next owner
        
        # Memory management - clear batch data
        del owner_batch
        if i % (batch_size * 10) == 0:
            gc.collect()
            logger.info(f"Processed {i}/{len(owners_df)} owners")
    
    matches_df = pd.DataFrame(matches)
    
    # Get unmatched records
    unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"Lightweight deterministic matching: {len(matches_df)} exact matches")
    return matches_df, unmatched_owners, unmatched_transactions

def lightweight_fuzzy_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.8  # Higher threshold for fewer matches
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Lightweight fuzzy matching with minimal resource usage.
    """
    logger.info("Starting lightweight fuzzy matching")
    
    if len(owners_df) == 0 or len(transactions_df) == 0:
        return pd.DataFrame(), owners_df, transactions_df
    
    matches = []
    matched_owner_ids = set()
    matched_txn_ids = set()
    
    # Process in very small batches
    batch_size = 50  # Even smaller batches
    
    for i in range(0, len(owners_df), batch_size):
        owner_batch = owners_df.iloc[i:i+batch_size]
        
        for _, owner_row in owner_batch.iterrows():
            owner_building = str(owner_row.get('building_clean', '')).lower().strip()
            owner_unit = str(owner_row.get('unit_no', '')).strip()
            owner_area = owner_row.get('area_sqm', 0)
            
            best_match = None
            best_score = 0
            
            # Check only first 1000 transactions to save time
            for _, txn_row in transactions_df.head(1000).iterrows():
                txn_building = str(txn_row.get('building_clean', '')).lower().strip()
                txn_unit = str(txn_row.get('unit_no', '')).strip()
                txn_area = txn_row.get('area_sqm', 0)
                
                # Simple similarity scoring
                building_score = 0.0
                if owner_building == txn_building:
                    building_score = 1.0
                elif owner_building in txn_building or txn_building in owner_building:
                    building_score = 0.8
                
                unit_score = 1.0 if owner_unit == txn_unit else 0.0
                
                area_score = 0.0
                if owner_area > 0 and txn_area > 0:
                    area_diff_pct = abs(owner_area - txn_area) / owner_area
                    if area_diff_pct < 0.05:  # Within 5%
                        area_score = 1.0 - area_diff_pct
                
                total_score = (0.6 * building_score + 0.3 * unit_score + 0.1 * area_score)
                
                if total_score > best_score and total_score >= min_score:
                    best_score = total_score
                    best_match = txn_row
            
            if best_match is not None:
                matches.append({
                    'owner_id': owner_row['owner_id'],
                    'txn_id': best_match['txn_id'],
                    'match_type': 'fuzzy',
                    'confidence': best_score,
                    'confidence_bucket': 'High' if best_score >= 0.9 else 'Medium',
                    'match_score': best_score
                })
                
                matched_owner_ids.add(owner_row['owner_id'])
                matched_txn_ids.add(best_match['txn_id'])
        
        # Memory management
        del owner_batch
        if i % (batch_size * 5) == 0:
            gc.collect()
            logger.info(f"Processed {i}/{len(owners_df)} owners for fuzzy matching")
    
    matches_df = pd.DataFrame(matches)
    
    # Get unmatched records
    unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"Lightweight fuzzy matching: {len(matches_df)} matches")
    return matches_df, unmatched_owners, unmatched_transactions

def run_lightweight_pipeline(
    owners_file: str,
    transactions_file: str,
    output_dir: str = "data/processed",
    run_id: str = None
) -> Dict[str, Any]:
    """
    Run the lightweight matching pipeline with minimal resource usage.
    """
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"Starting lightweight pipeline: {run_id}")
    
    # Check available resources
    available_cores, available_memory = get_available_resources()
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load data with minimal memory usage
    logger.info("Loading data with minimal memory usage")
    from .preprocess import preprocess_owners, preprocess_transactions
    
    # Load with chunked reading to minimize memory
    owners_df = pd.read_csv(owners_file, low_memory=True) if owners_file.endswith('.csv') else pd.read_excel(owners_file)
    transactions_df = pd.read_csv(transactions_file, low_memory=True) if transactions_file.endswith('.csv') else pd.read_excel(transactions_file)
    
    # Preprocess data
    owners_clean = preprocess_owners(owners_df)
    transactions_clean = preprocess_transactions(transactions_df)
    
    # Free original dataframes
    del owners_df, transactions_df
    gc.collect()
    
    # Lightweight deterministic matching
    logger.info("Running lightweight deterministic matching")
    tier1_matches, unmatched_owners, unmatched_transactions = lightweight_deterministic_match(
        owners_clean, transactions_clean
    )
    
    # Lightweight fuzzy matching
    logger.info("Running lightweight fuzzy matching")
    tier2_matches, final_unmatched_owners, final_unmatched_transactions = lightweight_fuzzy_match(
        unmatched_owners, unmatched_transactions
    )
    
    # Combine all matches
    all_matches = pd.concat([tier1_matches, tier2_matches], ignore_index=True)
    
    # Save results with minimal I/O
    logger.info("Saving lightweight pipeline results")
    matches_path = os.path.join(output_dir, "lightweight_matches.parquet")
    all_matches.to_parquet(matches_path, index=False, compression='snappy')
    
    # Save unmatched records
    if len(final_unmatched_owners) > 0:
        unmatched_owners_path = os.path.join(output_dir, "lightweight_unmatched_owners.csv")
        final_unmatched_owners.to_csv(unmatched_owners_path, index=False)
    
    if len(final_unmatched_transactions) > 0:
        unmatched_txns_path = os.path.join(output_dir, "lightweight_unmatched_transactions.csv")
        final_unmatched_transactions.to_csv(unmatched_txns_path, index=False)
    
    # Compile statistics
    total_owners = len(owners_clean)
    total_transactions = len(transactions_clean)
    total_matches = len(all_matches)
    tier1_count = len(tier1_matches)
    tier2_count = len(tier2_matches)
    
    stats = {
        'run_id': run_id,
        'timestamp': datetime.now().isoformat(),
        'pipeline_type': 'lightweight',
        'resource_usage': {
            'cores_used': available_cores,
            'memory_gb': available_memory,
            'memory_efficient': True
        },
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
    
    logger.info(f"Lightweight pipeline completed: {total_matches} matches")
    return stats 
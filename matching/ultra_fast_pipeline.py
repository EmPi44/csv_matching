"""
Ultra-fast matching pipeline for Dubai Hills property data.

Uses aggressive chunking, maximum parallelism, and optimized algorithms.
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, Tuple, List
from loguru import logger
import os
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing as mp
from functools import partial
import hashlib
import math


def chunk_dataframe(df: pd.DataFrame, chunk_size: int = 5000) -> List[pd.DataFrame]:
    """
    Split DataFrame into smaller chunks for parallel processing.
    
    Args:
        df: DataFrame to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of DataFrame chunks
    """
    chunks = []
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size].copy()
        chunks.append(chunk)
    return chunks


def ultra_fast_deterministic_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    tolerance_pct: float = 0.02
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Ultra-fast deterministic matching using vectorized operations.
    
    Args:
        owners_df: Preprocessed owners DataFrame
        transactions_df: Preprocessed transactions DataFrame
        tolerance_pct: Area tolerance percentage
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting ultra-fast deterministic matching")
    
    # Create copies
    owners = owners_df.copy()
    transactions = transactions_df.copy()
    
    # Add unique IDs
    owners['owner_id'] = range(len(owners))
    transactions['txn_id'] = range(len(transactions))
    
    # Create composite keys for exact matching
    owners['composite_key'] = (
        owners['project_clean'].astype(str) + '|' +
        owners['building_clean'].astype(str) + '|' +
        owners['unit_no'].astype(str)
    )
    
    transactions['composite_key'] = (
        transactions['project_clean'].astype(str) + '|' +
        transactions['building_clean'].astype(str) + '|' +
        transactions['unit_no'].astype(str)
    )
    
    # Use pandas merge for exact matching (much faster than loops)
    exact_matches = pd.merge(
        owners, transactions, 
        on='composite_key', 
        how='inner',
        suffixes=('_owner', '_txn')
    )
    
    # Apply area tolerance filter
    if len(exact_matches) > 0:
        area_owner = exact_matches['area_sqm_owner']
        area_txn = exact_matches['area_sqm_txn']
        
        # Vectorized area tolerance check
        area_diff_pct = np.abs(area_owner - area_txn) / area_owner
        area_mask = (area_diff_pct <= tolerance_pct) & (area_owner > 0)
        
        exact_matches = exact_matches[area_mask]
        
        # Keep only first match per owner (if multiple)
        exact_matches = exact_matches.drop_duplicates(subset=['owner_id'], keep='first')
        
        # Create final matches DataFrame
        matches_df = pd.DataFrame({
            'owner_id': exact_matches['owner_id'],
            'txn_id': exact_matches['txn_id'],
            'match_type': 'exact',
            'confidence': 1.0,
            'confidence_bucket': 'High',
            'match_score': 1.0
        })
        
        logger.info(f"Found {len(matches_df)} exact matches")
    else:
        matches_df = pd.DataFrame()
        logger.info("No exact matches found")
    
    # Get unmatched records
    matched_owner_ids = set(matches_df['owner_id'].unique()) if len(matches_df) > 0 else set()
    matched_txn_ids = set(matches_df['txn_id'].unique()) if len(matches_df) > 0 else set()
    
    unmatched_owners = owners[~owners['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions[~transactions['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"Unmatched: {len(unmatched_owners)} owners, {len(unmatched_transactions)} transactions")
    
    return matches_df, unmatched_owners, unmatched_transactions


def ultra_fast_fuzzy_match_batch(
    owners_batch: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    max_candidates: int = 2
) -> List[Dict]:
    """
    Ultra-fast fuzzy matching for a batch of owners.
    
    Args:
        owners_batch: Batch of owners to match
        transactions_df: All transactions DataFrame
        min_score: Minimum score threshold
        max_candidates: Maximum candidates per owner
        
    Returns:
        List of match dictionaries
    """
    matches = []
    
    # Pre-compute building indices for faster lookup
    building_groups = transactions_df.groupby('building_clean')
    
    for _, owner_row in owners_batch.iterrows():
        owner_building = str(owner_row['building_clean']).strip().lower()
        owner_area = owner_row['area_sqm']
        owner_unit = str(owner_row['unit_no']).strip()
        
        # Find similar buildings using fast string operations
        candidates = []
        
        for building_name, building_txns in building_groups:
            if pd.isna(building_name):
                continue
                
            txn_building = str(building_name).strip().lower()
            
            # Quick building similarity check (optimized)
            if (owner_building == txn_building or
                owner_building in txn_building or 
                txn_building in owner_building or
                len(set(owner_building.split()) & set(txn_building.split())) >= 1):
                
                for _, txn_row in building_txns.iterrows():
                    # Calculate scores (simplified for speed)
                    building_score = 0.9 if owner_building == txn_building else 0.7
                    unit_score = 1.0 if owner_unit == str(txn_row['unit_no']).strip() else 0.0
                    
                    # Area score (simplified)
                    txn_area = txn_row['area_sqm']
                    if pd.notna(owner_area) and pd.notna(txn_area) and owner_area > 0:
                        area_diff_pct = abs(owner_area - txn_area) / owner_area
                        area_score = max(0.0, 1.0 - (area_diff_pct / 0.02))
                    else:
                        area_score = 0.0
                    
                    # Overall score
                    total_score = (0.6 * building_score + 
                                  0.3 * unit_score + 
                                  0.1 * area_score)
                    
                    if total_score >= min_score:
                        candidates.append({
                            'txn_idx': txn_row.name,  # Use index for faster lookup
                            'score': total_score
                        })
        
        # Sort by score and take top candidates
        candidates.sort(key=lambda x: x['score'], reverse=True)
        candidates = candidates[:max_candidates]
        
        # Add matches
        for candidate in candidates:
            txn_row = transactions_df.loc[candidate['txn_idx']]
            matches.append({
                'owner_id': owner_row['owner_id'],
                'txn_id': txn_row['txn_id'],
                'match_type': 'fuzzy',
                'confidence': candidate['score'],
                'confidence_bucket': 'High' if candidate['score'] >= 0.9 else 'Medium',
                'match_score': candidate['score']
            })
    
    return matches


def ultra_fast_fuzzy_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    chunk_size: int = 1000,
    max_workers: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Ultra-fast fuzzy matching using aggressive chunking and parallelism.
    
    Args:
        owners_df: Unmatched owners DataFrame
        transactions_df: Unmatched transactions DataFrame
        min_score: Minimum score threshold
        chunk_size: Size of owner chunks
        max_workers: Number of parallel workers
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting ultra-fast fuzzy matching")
    
    if len(owners_df) == 0 or len(transactions_df) == 0:
        logger.info("No records to match in fuzzy matching")
        return pd.DataFrame(), owners_df, transactions_df
    
    # Determine number of workers (use all available cores)
    if max_workers is None:
        max_workers = mp.cpu_count()
    
    # Split owners into smaller chunks for better parallelism
    owner_chunks = chunk_dataframe(owners_df, chunk_size)
    
    logger.info(f"Processing {len(owner_chunks)} chunks with {max_workers} workers")
    
    # Process chunks in parallel
    all_matches = []
    
    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all chunks
            future_to_chunk = {}
            for i, chunk in enumerate(owner_chunks):
                future = executor.submit(
                    ultra_fast_fuzzy_match_batch,
                    chunk, transactions_df, min_score
                )
                future_to_chunk[future] = i
            
            # Collect results
            for future in as_completed(future_to_chunk):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_matches = future.result()
                    all_matches.extend(chunk_matches)
                    logger.info(f"Completed chunk {chunk_idx + 1}/{len(owner_chunks)}")
                except Exception as e:
                    logger.error(f"Error in chunk {chunk_idx}: {e}")
    else:
        # Sequential processing
        for i, chunk in enumerate(owner_chunks):
            chunk_matches = ultra_fast_fuzzy_match_batch(
                chunk, transactions_df, min_score
            )
            all_matches.extend(chunk_matches)
            logger.info(f"Completed chunk {i + 1}/{len(owner_chunks)}")
    
    # Create matches DataFrame
    if all_matches:
        matches_df = pd.DataFrame(all_matches)
        
        # Remove duplicates (keep highest score)
        matches_df = matches_df.sort_values('match_score', ascending=False)
        matches_df = matches_df.drop_duplicates(subset=['owner_id', 'txn_id'], keep='first')
        
        logger.info(f"Found {len(matches_df)} fuzzy matches")
    else:
        matches_df = pd.DataFrame()
        logger.info("No fuzzy matches found")
    
    # Get unmatched records
    matched_owner_ids = set(matches_df['owner_id'].unique()) if len(matches_df) > 0 else set()
    matched_txn_ids = set(matches_df['txn_id'].unique()) if len(matches_df) > 0 else set()
    
    unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"Unmatched: {len(unmatched_owners)} owners, {len(unmatched_transactions)} transactions")
    
    return matches_df, unmatched_owners, unmatched_transactions


def run_ultra_fast_pipeline(
    owners_file: str,
    transactions_file: str,
    output_dir: str = "data/processed",
    run_id: str = None,
    chunk_size: int = 1000
) -> Dict[str, Any]:
    """
    Run the ultra-fast matching pipeline.
    
    Args:
        owners_file: Path to owners data file
        transactions_file: Path to transactions data file
        output_dir: Directory for pipeline outputs
        run_id: Optional run ID
        chunk_size: Size of chunks for parallel processing
        
    Returns:
        Dictionary with pipeline results and statistics
    """
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"Starting ultra-fast matching pipeline run: {run_id}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load and preprocess data
    logger.info("Loading and preprocessing data")
    from .preprocess import preprocess_owners, preprocess_transactions
    
    # Load with optimized settings
    owners_df = pd.read_csv(owners_file, low_memory=False) if owners_file.endswith('.csv') else pd.read_excel(owners_file)
    transactions_df = pd.read_csv(transactions_file, low_memory=False) if transactions_file.endswith('.csv') else pd.read_excel(transactions_file)
    
    owners_clean = preprocess_owners(owners_df)
    transactions_clean = preprocess_transactions(transactions_df)
    
    # Ultra-fast deterministic matching
    logger.info("Running ultra-fast deterministic matching")
    tier1_matches, unmatched_owners, unmatched_transactions = ultra_fast_deterministic_match(
        owners_clean, transactions_clean
    )
    
    # Ultra-fast fuzzy matching
    logger.info("Running ultra-fast fuzzy matching")
    tier2_matches, final_unmatched_owners, final_unmatched_transactions = ultra_fast_fuzzy_match(
        unmatched_owners, unmatched_transactions, chunk_size=chunk_size
    )
    
    # Combine all matches
    all_matches = pd.concat([tier1_matches, tier2_matches], ignore_index=True)
    
    # Write outputs
    logger.info("Writing pipeline outputs")
    matches_path = os.path.join(output_dir, "ultra_fast_matches.parquet")
    all_matches.to_parquet(matches_path, index=False)
    
    if len(final_unmatched_owners) > 0:
        unmatched_owners_path = os.path.join(output_dir, "ultra_fast_owners_unmatched.csv")
        final_unmatched_owners.to_csv(unmatched_owners_path, index=False)
    
    if len(final_unmatched_transactions) > 0:
        unmatched_txns_path = os.path.join(output_dir, "ultra_fast_transactions_unmatched.csv")
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
    
    logger.info(f"Ultra-fast pipeline completed: {total_matches} matches from {total_owners} owners and {total_transactions} transactions")
    return stats 
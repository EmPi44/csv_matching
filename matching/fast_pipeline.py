"""
High-performance matching pipeline for Dubai Hills property data.

Uses vectorized operations, indexing, and parallel processing for speed.
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


def create_fast_index(df: pd.DataFrame, key_columns: List[str]) -> Dict[str, List[int]]:
    """
    Create a fast lookup index for exact matching.
    
    Args:
        df: DataFrame to index
        key_columns: Columns to use for indexing
        
    Returns:
        Dictionary mapping composite keys to row indices
    """
    # Create composite key
    composite_key = df[key_columns].astype(str).agg('|'.join, axis=1)
    
    # Create index
    index = {}
    for idx, key in enumerate(composite_key):
        if key not in index:
            index[key] = []
        index[key].append(idx)
    
    return index


def fast_deterministic_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    tolerance_pct: float = 0.02
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fast deterministic matching using indexing.
    
    Args:
        owners_df: Preprocessed owners DataFrame
        transactions_df: Preprocessed transactions DataFrame
        tolerance_pct: Area tolerance percentage
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting fast deterministic matching")
    
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
    
    # Create index for transactions
    txn_index = create_fast_index(transactions, ['composite_key'])
    
    # Find exact matches
    exact_matches = []
    matched_owner_ids = set()
    matched_txn_ids = set()
    
    for _, owner_row in owners.iterrows():
        key = owner_row['composite_key']
        if key in txn_index:
            # Check area tolerance for each potential match
            for txn_idx in txn_index[key]:
                txn_row = transactions.iloc[txn_idx]
                
                # Area tolerance check
                area_owner = owner_row['area_sqm']
                area_txn = txn_row['area_sqm']
                
                if pd.notna(area_owner) and pd.notna(area_txn) and area_owner > 0:
                    area_diff_pct = abs(area_owner - area_txn) / area_owner
                    if area_diff_pct <= tolerance_pct:
                        exact_matches.append({
                            'owner_id': owner_row['owner_id'],
                            'txn_id': txn_row['txn_id'],
                            'match_type': 'exact',
                            'confidence': 1.0,
                            'confidence_bucket': 'High',
                            'match_score': 1.0
                        })
                        matched_owner_ids.add(owner_row['owner_id'])
                        matched_txn_ids.add(txn_row['txn_id'])
                        break  # Take first match
    
    # Create matches DataFrame
    if exact_matches:
        matches_df = pd.DataFrame(exact_matches)
        logger.info(f"Found {len(matches_df)} exact matches")
    else:
        matches_df = pd.DataFrame()
        logger.info("No exact matches found")
    
    # Get unmatched records
    unmatched_owners = owners[~owners['owner_id'].isin(matched_owner_ids)].copy()
    unmatched_transactions = transactions[~transactions['txn_id'].isin(matched_txn_ids)].copy()
    
    logger.info(f"Unmatched: {len(unmatched_owners)} owners, {len(unmatched_transactions)} transactions")
    
    return matches_df, unmatched_owners, unmatched_transactions


def create_building_index(df: pd.DataFrame) -> Dict[str, List[int]]:
    """
    Create building-level index for fuzzy matching.
    
    Args:
        df: DataFrame to index
        
    Returns:
        Dictionary mapping building names to row indices
    """
    building_index = {}
    for idx, building in enumerate(df['building_clean']):
        if pd.notna(building):
            building_key = str(building).strip().lower()
            if building_key not in building_index:
                building_index[building_key] = []
            building_index[building_key].append(idx)
    
    return building_index


def fast_fuzzy_match_batch(
    owners_batch: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    max_candidates: int = 3
) -> List[Dict]:
    """
    Fast fuzzy matching for a batch of owners.
    
    Args:
        owners_batch: Batch of owners to match
        transactions_df: All transactions DataFrame
        min_score: Minimum score threshold
        max_candidates: Maximum candidates per owner
        
    Returns:
        List of match dictionaries
    """
    matches = []
    
    # Create building index for transactions
    txn_building_index = create_building_index(transactions_df)
    
    for _, owner_row in owners_batch.iterrows():
        owner_building = str(owner_row['building_clean']).strip().lower()
        owner_area = owner_row['area_sqm']
        owner_unit = str(owner_row['unit_no']).strip()
        
        # Find similar buildings using simple string operations
        candidates = []
        
        for txn_building, txn_indices in txn_building_index.items():
            # Quick building similarity check
            if (owner_building in txn_building or 
                txn_building in owner_building or
                len(set(owner_building.split()) & set(txn_building.split())) >= 2):
                
                for txn_idx in txn_indices:
                    txn_row = transactions_df.iloc[txn_idx]
                    
                    # Calculate scores
                    building_score = 0.8 if owner_building == txn_building else 0.6
                    unit_score = 1.0 if owner_unit == str(txn_row['unit_no']).strip() else 0.0
                    
                    # Area score
                    txn_area = txn_row['area_sqm']
                    if pd.notna(owner_area) and pd.notna(txn_area) and owner_area > 0:
                        area_diff_pct = abs(owner_area - txn_area) / owner_area
                        area_score = max(0.0, 1.0 - (area_diff_pct / 0.02))
                    else:
                        area_score = 0.0
                    
                    # Overall score
                    total_score = (0.5 * building_score + 
                                  0.3 * unit_score + 
                                  0.2 * area_score)
                    
                    if total_score >= min_score:
                        candidates.append({
                            'txn_idx': txn_idx,
                            'score': total_score
                        })
        
        # Sort by score and take top candidates
        candidates.sort(key=lambda x: x['score'], reverse=True)
        candidates = candidates[:max_candidates]
        
        # Add matches
        for candidate in candidates:
            txn_row = transactions_df.iloc[candidate['txn_idx']]
            matches.append({
                'owner_id': owner_row['owner_id'],
                'txn_id': txn_row['txn_id'],
                'match_type': 'fuzzy',
                'confidence': candidate['score'],
                'confidence_bucket': 'High' if candidate['score'] >= 0.9 else 'Medium',
                'match_score': candidate['score']
            })
    
    return matches


def fast_fuzzy_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    batch_size: int = 1000,
    max_workers: int = None
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Fast fuzzy matching using parallel processing.
    
    Args:
        owners_df: Unmatched owners DataFrame
        transactions_df: Unmatched transactions DataFrame
        min_score: Minimum score threshold
        batch_size: Size of batches for parallel processing
        max_workers: Number of parallel workers
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting fast fuzzy matching")
    
    if len(owners_df) == 0 or len(transactions_df) == 0:
        logger.info("No records to match in fuzzy matching")
        return pd.DataFrame(), owners_df, transactions_df
    
    # Determine number of workers
    if max_workers is None:
        max_workers = min(mp.cpu_count(), 8)  # Cap at 8 workers
    
    # Split owners into batches
    owner_batches = []
    for i in range(0, len(owners_df), batch_size):
        batch = owners_df.iloc[i:i+batch_size].copy()
        owner_batches.append(batch)
    
    logger.info(f"Processing {len(owner_batches)} batches with {max_workers} workers")
    
    # Process batches in parallel
    all_matches = []
    
    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Create partial function with fixed arguments
            match_func = partial(
                fast_fuzzy_match_batch,
                transactions_df=transactions_df,
                min_score=min_score
            )
            
            # Submit all batches
            future_to_batch = {
                executor.submit(match_func, batch): i 
                for i, batch in enumerate(owner_batches)
            }
            
            # Collect results
            for future in as_completed(future_to_batch):
                batch_idx = future_to_batch[future]
                try:
                    batch_matches = future.result()
                    all_matches.extend(batch_matches)
                    logger.info(f"Completed batch {batch_idx + 1}/{len(owner_batches)}")
                except Exception as e:
                    logger.error(f"Error in batch {batch_idx}: {e}")
    else:
        # Sequential processing
        for i, batch in enumerate(owner_batches):
            batch_matches = fast_fuzzy_match_batch(
                batch, transactions_df, min_score
            )
            all_matches.extend(batch_matches)
            logger.info(f"Completed batch {i + 1}/{len(owner_batches)}")
    
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
    if len(matches_df) > 0:
        matched_owner_ids = set(matches_df['owner_id'].unique())
        matched_txn_ids = set(matches_df['txn_id'].unique())
        
        unmatched_owners = owners_df[~owners_df['owner_id'].isin(matched_owner_ids)].copy()
        unmatched_transactions = transactions_df[~transactions_df['txn_id'].isin(matched_txn_ids)].copy()
    else:
        unmatched_owners = owners_df.copy()
        unmatched_transactions = transactions_df.copy()
    
    logger.info(f"Unmatched: {len(unmatched_owners)} owners, {len(unmatched_transactions)} transactions")
    
    return matches_df, unmatched_owners, unmatched_transactions


def run_fast_pipeline(
    owners_file: str,
    transactions_file: str,
    output_dir: str = "data/processed",
    run_id: str = None
) -> Dict[str, Any]:
    """
    Run the fast matching pipeline.
    
    Args:
        owners_file: Path to owners data file
        transactions_file: Path to transactions data file
        output_dir: Directory for pipeline outputs
        run_id: Optional run ID
        
    Returns:
        Dictionary with pipeline results and statistics
    """
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"Starting fast matching pipeline run: {run_id}")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Load and preprocess data
    logger.info("Loading and preprocessing data")
    from .preprocess import preprocess_owners, preprocess_transactions
    
    owners_df = pd.read_csv(owners_file) if owners_file.endswith('.csv') else pd.read_excel(owners_file)
    transactions_df = pd.read_csv(transactions_file) if transactions_file.endswith('.csv') else pd.read_excel(transactions_file)
    
    owners_clean = preprocess_owners(owners_df)
    transactions_clean = preprocess_transactions(transactions_df)
    
    # Fast deterministic matching
    logger.info("Running fast deterministic matching")
    tier1_matches, unmatched_owners, unmatched_transactions = fast_deterministic_match(
        owners_clean, transactions_clean
    )
    
    # Fast fuzzy matching
    logger.info("Running fast fuzzy matching")
    tier2_matches, final_unmatched_owners, final_unmatched_transactions = fast_fuzzy_match(
        unmatched_owners, unmatched_transactions
    )
    
    # Combine all matches
    all_matches = pd.concat([tier1_matches, tier2_matches], ignore_index=True)
    
    # Write outputs
    logger.info("Writing pipeline outputs")
    matches_path = os.path.join(output_dir, "fast_matches.parquet")
    all_matches.to_parquet(matches_path, index=False)
    
    if len(final_unmatched_owners) > 0:
        unmatched_owners_path = os.path.join(output_dir, "fast_owners_unmatched.csv")
        final_unmatched_owners.to_csv(unmatched_owners_path, index=False)
    
    if len(final_unmatched_transactions) > 0:
        unmatched_txns_path = os.path.join(output_dir, "fast_transactions_unmatched.csv")
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
    
    logger.info(f"Fast pipeline completed: {total_matches} matches from {total_owners} owners and {total_transactions} transactions")
    return stats 
"""
Tier 1: Deterministic matching for Dubai Hills property pipeline.

Performs exact joins on composite keys with area validation.
"""

import pandas as pd
from typing import Tuple, Dict, Any
from loguru import logger


def tier1_deterministic_match(
    owners_df: pd.DataFrame, 
    transactions_df: pd.DataFrame,
    area_tolerance_pct: float = 0.01
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform Tier 1 deterministic matching.
    
    Args:
        owners_df: Preprocessed owner DataFrame
        transactions_df: Preprocessed transaction DataFrame
        area_tolerance_pct: Maximum allowed area difference (default 1%)
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting Tier 1 deterministic matching")
    
    # Ensure required columns exist
    required_cols = ['composite_key', 'area_sqm']
    for col in required_cols:
        if col not in owners_df.columns:
            raise ValueError(f"Missing required column '{col}' in owners DataFrame")
        if col not in transactions_df.columns:
            raise ValueError(f"Missing required column '{col}' in transactions DataFrame")
    
    # Create copies to avoid modifying originals
    owners = owners_df.copy()
    transactions = transactions_df.copy()
    
    # Add unique IDs if not present
    if 'owner_id' not in owners.columns:
        owners['owner_id'] = range(len(owners))
    if 'txn_id' not in transactions.columns:
        transactions['txn_id'] = range(len(transactions))
    
    # Perform exact join on composite key
    logger.info("Performing exact join on composite keys")
    matches = pd.merge(
        owners, 
        transactions, 
        on='composite_key', 
        how='inner',
        suffixes=('_owner', '_txn')
    )
    
    logger.info(f"Found {len(matches)} exact composite key matches")
    
    # Filter by area tolerance
    if len(matches) > 0:
        # Calculate area difference percentage
        matches['area_diff_pct'] = abs(
            matches['area_sqm_owner'] - matches['area_sqm_txn']
        ) / matches['area_sqm_owner'].replace(0, 1)
        
        # Filter by tolerance
        area_filter = matches['area_diff_pct'] <= area_tolerance_pct
        matches = matches[area_filter]
        
        logger.info(f"After area filtering: {len(matches)} matches")
    
    # Prepare final matches DataFrame
    if len(matches) > 0:
        final_matches = pd.DataFrame({
            'owner_id': matches['owner_id'],
            'txn_id': matches['txn_id'],
            'match_confidence': 1.0,  # Perfect confidence for deterministic matches
            'confidence_bucket': 'High',
            'review_status': 'auto_approved',
            'pipeline_run_id': pd.Timestamp.now().strftime('%Y%m%d_%H%M%S'),
            'match_method': 'tier1_deterministic',
            'area_diff_pct': matches.get('area_diff_pct', 0.0)
        })
    else:
        final_matches = pd.DataFrame(columns=[
            'owner_id', 'txn_id', 'match_confidence', 'confidence_bucket',
            'review_status', 'pipeline_run_id', 'match_method', 'area_diff_pct'
        ])
    
    # Identify unmatched records
    matched_owner_ids = set(matches['owner_id']) if len(matches) > 0 else set()
    matched_txn_ids = set(matches['txn_id']) if len(matches) > 0 else set()
    
    unmatched_owners = owners[~owners['owner_id'].isin(matched_owner_ids)]
    unmatched_transactions = transactions[~transactions['txn_id'].isin(matched_txn_ids)]
    
    logger.info(f"Tier 1 complete: {len(final_matches)} matches, "
                f"{len(unmatched_owners)} unmatched owners, "
                f"{len(unmatched_transactions)} unmatched transactions")
    
    return final_matches, unmatched_owners, unmatched_transactions


def validate_deterministic_matches(
    matches_df: pd.DataFrame,
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Validate Tier 1 matches for quality assurance.
    
    Args:
        matches_df: Matches from Tier 1
        owners_df: Original owner DataFrame
        transactions_df: Original transaction DataFrame
        
    Returns:
        Dictionary with validation statistics
    """
    if len(matches_df) == 0:
        return {
            'total_matches': 0,
            'avg_confidence': 0.0,
            'area_consistency': 0.0,
            'duplicate_owners': 0,
            'duplicate_transactions': 0
        }
    
    # Check for duplicate matches
    duplicate_owners = matches_df['owner_id'].duplicated().sum()
    duplicate_transactions = matches_df['txn_id'].duplicated().sum()
    
    # Calculate area consistency (should be very high for deterministic matches)
    area_consistency = (
        matches_df['area_diff_pct'] <= 0.01
    ).mean() if 'area_diff_pct' in matches_df.columns else 1.0
    
    validation_stats = {
        'total_matches': len(matches_df),
        'avg_confidence': matches_df['match_confidence'].mean(),
        'area_consistency': area_consistency,
        'duplicate_owners': duplicate_owners,
        'duplicate_transactions': duplicate_transactions,
        'match_rate_owners': len(matches_df) / len(owners_df) if len(owners_df) > 0 else 0,
        'match_rate_transactions': len(matches_df) / len(transactions_df) if len(transactions_df) > 0 else 0
    }
    
    logger.info(f"Tier 1 validation: {validation_stats}")
    return validation_stats 
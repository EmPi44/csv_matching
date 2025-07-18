"""
Tier 2: Fuzzy matching for Dubai Hills property pipeline.

Performs fuzzy string matching with scoring for non-exact matches.
"""

import pandas as pd
import numpy as np
from rapidfuzz import fuzz
from typing import Tuple, Dict, Any, List
from loguru import logger


def calculate_building_similarity(building_owner: str, building_txn: str) -> float:
    """
    Calculate building name similarity using token set ratio.
    
    Args:
        building_owner: Building name from owner record
        building_txn: Building name from transaction record
        
    Returns:
        Similarity score between 0 and 1
    """
    if pd.isna(building_owner) or pd.isna(building_txn):
        return 0.0
    
    building_owner = str(building_owner).strip()
    building_txn = str(building_txn).strip()
    
    if not building_owner or not building_txn:
        return 0.0
    
    # Use token set ratio for building names
    similarity = fuzz.token_set_ratio(building_owner, building_txn)
    return similarity / 100.0


def calculate_unit_match(unit_owner: str, unit_txn: str) -> float:
    """
    Calculate unit number match score.
    
    Args:
        unit_owner: Unit number from owner record
        unit_txn: Unit number from transaction record
        
    Returns:
        1.0 if exact match, 0.0 otherwise
    """
    if pd.isna(unit_owner) or pd.isna(unit_txn):
        return 0.0
    
    unit_owner = str(unit_owner).strip()
    unit_txn = str(unit_txn).strip()
    
    return 1.0 if unit_owner == unit_txn else 0.0


def calculate_area_score(area_owner: float, area_txn: float) -> float:
    """
    Calculate area similarity score.
    
    Args:
        area_owner: Area from owner record (sqm)
        area_txn: Area from transaction record (sqm)
        
    Returns:
        Area similarity score between 0 and 1
    """
    if pd.isna(area_owner) or pd.isna(area_txn) or area_owner == 0:
        return 0.0
    
    area_diff_pct = abs(area_owner - area_txn) / area_owner
    
    # Linear score: 1.0 at 0% difference, 0.0 at 2% difference
    score = max(0.0, 1.0 - (area_diff_pct / 0.02))
    return score


def calculate_fuzzy_score(
    building_sim: float,
    unit_match: float,
    area_score: float,
    weights: Dict[str, float] = None
) -> float:
    """
    Calculate overall fuzzy matching score.
    
    Args:
        building_sim: Building similarity score (0-1)
        unit_match: Unit match score (0-1)
        area_score: Area similarity score (0-1)
        weights: Weights for each component (default: 0.5, 0.3, 0.2)
        
    Returns:
        Overall fuzzy score between 0 and 1
    """
    if weights is None:
        weights = {'building': 0.5, 'unit': 0.3, 'area': 0.2}
    
    score = (
        weights['building'] * building_sim +
        weights['unit'] * unit_match +
        weights['area'] * area_score
    )
    
    return round(score, 4)


def assign_confidence_bucket(score: float) -> str:
    """
    Assign confidence bucket based on score.
    
    Args:
        score: Fuzzy matching score (0-1)
        
    Returns:
        Confidence bucket: 'High', 'Medium', 'Low', or 'Reject'
    """
    if score >= 0.90:
        return 'High'
    elif score >= 0.85:
        return 'Medium'
    elif score >= 0.75:
        return 'Low'
    else:
        return 'Reject'


def tier2_fuzzy_match(
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    min_score: float = 0.75,
    max_candidates_per_owner: int = 5
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Perform Tier 2 fuzzy matching.
    
    Args:
        owners_df: Unmatched owners from Tier 1
        transactions_df: Unmatched transactions from Tier 1
        min_score: Minimum score to consider a match
        max_candidates_per_owner: Maximum candidates to keep per owner
        
    Returns:
        Tuple of (matches_df, unmatched_owners_df, unmatched_transactions_df)
    """
    logger.info("Starting Tier 2 fuzzy matching")
    
    if len(owners_df) == 0 or len(transactions_df) == 0:
        logger.info("No records to match in Tier 2")
        return pd.DataFrame(), owners_df, transactions_df
    
    # Ensure required columns exist
    required_cols = ['project_clean', 'building_clean', 'unit_no', 'area_sqm']
    for col in required_cols:
        if col not in owners_df.columns:
            raise ValueError(f"Missing required column '{col}' in owners DataFrame")
        if col not in transactions_df.columns:
            raise ValueError(f"Missing required column '{col}' in transactions DataFrame")
    
    # Create copies
    owners = owners_df.copy()
    transactions = transactions_df.copy()
    
    # Add unique IDs if not present
    if 'owner_id' not in owners.columns:
        owners['owner_id'] = range(len(owners))
    if 'txn_id' not in transactions.columns:
        transactions['txn_id'] = range(len(transactions))
    
    # Block by project to reduce comparison space
    logger.info("Blocking by project for fuzzy matching")
    all_matches = []
    
    # Group by project for blocking
    owner_groups = owners.groupby('project_clean')
    txn_groups = transactions.groupby('project_clean')
    
    for project, project_owners in owner_groups:
        if project not in txn_groups.groups:
            continue
            
        project_txns = txn_groups.get_group(project)
        logger.info(f"Processing project '{project}': {len(project_owners)} owners, {len(project_txns)} transactions")
        
        # Compare each owner with each transaction in the same project
        for _, owner_row in project_owners.iterrows():
            owner_candidates = []
            
            for _, txn_row in project_txns.iterrows():
                # Calculate similarity scores
                building_sim = calculate_building_similarity(
                    owner_row['building_clean'], 
                    txn_row['building_clean']
                )
                
                unit_match = calculate_unit_match(
                    owner_row['unit_no'], 
                    txn_row['unit_no']
                )
                
                area_score = calculate_area_score(
                    owner_row['area_sqm'], 
                    txn_row['area_sqm']
                )
                
                # Calculate overall score
                fuzzy_score = calculate_fuzzy_score(building_sim, unit_match, area_score)
                
                if fuzzy_score >= min_score:
                    owner_candidates.append({
                        'owner_id': owner_row['owner_id'],
                        'txn_id': txn_row['txn_id'],
                        'match_confidence': fuzzy_score,
                        'confidence_bucket': assign_confidence_bucket(fuzzy_score),
                        'building_sim': building_sim,
                        'unit_match': unit_match,
                        'area_score': area_score,
                        'project': project
                    })
            
            # Sort by score and keep top candidates
            if owner_candidates:
                owner_candidates.sort(key=lambda x: x['match_confidence'], reverse=True)
                all_matches.extend(owner_candidates[:max_candidates_per_owner])
    
    if not all_matches:
        logger.info("No fuzzy matches found")
        return pd.DataFrame(), owners, transactions
    
    # Convert to DataFrame
    matches_df = pd.DataFrame(all_matches)
    
    # Add metadata
    matches_df['review_status'] = 'pending_review'
    matches_df['pipeline_run_id'] = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
    matches_df['match_method'] = 'tier2_fuzzy'
    
    # Filter by confidence bucket
    accepted_matches = matches_df[matches_df['confidence_bucket'].isin(['High', 'Medium'])]
    review_matches = matches_df[matches_df['confidence_bucket'] == 'Low']
    
    logger.info(f"Tier 2 results: {len(accepted_matches)} accepted, {len(review_matches)} for review")
    
    # Identify unmatched records
    matched_owner_ids = set(accepted_matches['owner_id'])
    matched_txn_ids = set(accepted_matches['txn_id'])
    
    unmatched_owners = owners[~owners['owner_id'].isin(matched_owner_ids)]
    unmatched_transactions = transactions[~transactions['txn_id'].isin(matched_txn_ids)]
    
    return accepted_matches, unmatched_owners, unmatched_transactions


def get_fuzzy_match_stats(matches_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Get statistics for fuzzy matches.
    
    Args:
        matches_df: Fuzzy matches DataFrame
        
    Returns:
        Dictionary with match statistics
    """
    if len(matches_df) == 0:
        return {
            'total_matches': 0,
            'avg_confidence': 0.0,
            'bucket_distribution': {},
            'avg_building_sim': 0.0,
            'avg_unit_match': 0.0,
            'avg_area_score': 0.0
        }
    
    stats = {
        'total_matches': len(matches_df),
        'avg_confidence': matches_df['match_confidence'].mean(),
        'bucket_distribution': matches_df['confidence_bucket'].value_counts().to_dict(),
        'avg_building_sim': matches_df['building_sim'].mean(),
        'avg_unit_match': matches_df['unit_match'].mean(),
        'avg_area_score': matches_df['area_score'].mean(),
        'score_distribution': {
            '0.9+': (matches_df['match_confidence'] >= 0.9).sum(),
            '0.85-0.9': ((matches_df['match_confidence'] >= 0.85) & (matches_df['match_confidence'] < 0.9)).sum(),
            '0.75-0.85': ((matches_df['match_confidence'] >= 0.75) & (matches_df['match_confidence'] < 0.85)).sum()
        }
    }
    
    logger.info(f"Fuzzy match stats: {stats}")
    return stats 
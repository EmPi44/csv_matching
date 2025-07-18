"""
Review helpers for Tier 3 manual review process.

Handles exporting candidates for review and merging human decisions.
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, List
from loguru import logger


def export_review_candidates(
    fuzzy_matches_df: pd.DataFrame,
    owners_df: pd.DataFrame,
    transactions_df: pd.DataFrame,
    output_path: str = "data/review/pairs.parquet"
) -> pd.DataFrame:
    """
    Export candidates for manual review.
    
    Args:
        fuzzy_matches_df: Fuzzy matches from Tier 2
        owners_df: Owner DataFrame
        transactions_df: Transaction DataFrame
        output_path: Path to save review pairs
        
    Returns:
        DataFrame with pairs for review
    """
    logger.info("Exporting candidates for manual review")
    
    # Check if fuzzy matches DataFrame is empty
    if len(fuzzy_matches_df) == 0:
        logger.info("No fuzzy matches to export for review")
        return pd.DataFrame()
    
    # Filter for Low confidence matches
    review_candidates = fuzzy_matches_df[
        fuzzy_matches_df['confidence_bucket'] == 'Low'
    ].copy()
    
    if len(review_candidates) == 0:
        logger.info("No candidates for manual review")
        return pd.DataFrame()
    
    # Merge with owner and transaction details
    review_pairs = review_candidates.merge(
        owners_df[['owner_id', 'project_clean', 'building_clean', 'unit_no', 'area_sqm'] + 
                 [col for col in owners_df.columns if col.endswith('_clean')]],
        on='owner_id',
        how='left',
        suffixes=('', '_owner')
    )
    
    review_pairs = review_pairs.merge(
        transactions_df[['txn_id', 'project_clean', 'building_clean', 'unit_no', 'area_sqm'] + 
                       [col for col in transactions_df.columns if col.endswith('_clean')]],
        on='txn_id',
        how='left',
        suffixes=('_owner', '_txn')
    )
    
    # Add review metadata
    review_pairs['export_date'] = datetime.now().strftime('%Y-%m-%d')
    review_pairs['review_status'] = 'pending'
    review_pairs['reviewer_notes'] = ''
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Save to parquet
    review_pairs.to_parquet(output_path, index=False)
    
    logger.info(f"Exported {len(review_pairs)} pairs for manual review to {output_path}")
    
    return review_pairs


def load_review_decisions(
    decisions_path: str = None,
    review_dir: str = "data/review"
) -> pd.DataFrame:
    """
    Load human review decisions.
    
    Args:
        decisions_path: Specific decisions file path
        review_dir: Directory containing decision files
        
    Returns:
        DataFrame with review decisions
    """
    if decisions_path:
        if os.path.exists(decisions_path):
            decisions_df = pd.read_parquet(decisions_path)
            logger.info(f"Loaded {len(decisions_df)} decisions from {decisions_path}")
            return decisions_df
        else:
            logger.warning(f"Decisions file not found: {decisions_path}")
            return pd.DataFrame()
    
    # Look for decision files in review directory
    if not os.path.exists(review_dir):
        logger.warning(f"Review directory not found: {review_dir}")
        return pd.DataFrame()
    
    decision_files = [
        f for f in os.listdir(review_dir) 
        if f.startswith('decisions_') and f.endswith('.parquet')
    ]
    
    if not decision_files:
        logger.info("No decision files found")
        return pd.DataFrame()
    
    # Load most recent decision file
    decision_files.sort(reverse=True)
    latest_file = os.path.join(review_dir, decision_files[0])
    
    decisions_df = pd.read_parquet(latest_file)
    logger.info(f"Loaded {len(decisions_df)} decisions from {latest_file}")
    
    return decisions_df


def merge_review_decisions(
    fuzzy_matches_df: pd.DataFrame,
    decisions_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge human review decisions with fuzzy matches.
    
    Args:
        fuzzy_matches_df: Fuzzy matches from Tier 2
        decisions_df: Human review decisions
        
    Returns:
        Updated matches DataFrame with review decisions
    """
    logger.info("Merging human review decisions")
    
    if len(fuzzy_matches_df) == 0:
        logger.info("No fuzzy matches to merge decisions with")
        return fuzzy_matches_df
    
    if len(decisions_df) == 0:
        logger.info("No review decisions to merge")
        return fuzzy_matches_df
    
    # Create a copy to avoid modifying original
    matches = fuzzy_matches_df.copy()
    
    # Merge decisions
    matches = matches.merge(
        decisions_df[['owner_id', 'txn_id', 'review_status', 'reviewer_notes']],
        on=['owner_id', 'txn_id'],
        how='left',
        suffixes=('', '_review')
    )
    
    # Update review status for reviewed pairs
    reviewed_mask = matches['review_status_review'].notna()
    matches.loc[reviewed_mask, 'review_status'] = matches.loc[reviewed_mask, 'review_status_review']
    
    # Update confidence bucket for approved matches
    approved_mask = matches['review_status'] == 'approved'
    matches.loc[approved_mask, 'confidence_bucket'] = 'High'
    matches.loc[approved_mask, 'match_confidence'] = 0.95  # High confidence for approved
    
    # Remove rejected matches
    rejected_mask = matches['review_status'] == 'rejected'
    matches = matches[~rejected_mask]
    
    logger.info(f"Merged review decisions: {approved_mask.sum()} approved, {rejected_mask.sum()} rejected")
    
    return matches


def get_review_stats(
    review_pairs_df: pd.DataFrame,
    decisions_df: pd.DataFrame
) -> Dict[str, Any]:
    """
    Get statistics for review process.
    
    Args:
        review_pairs_df: Pairs exported for review
        decisions_df: Human review decisions
        
    Returns:
        Dictionary with review statistics
    """
    stats = {
        'total_pairs_exported': len(review_pairs_df),
        'total_decisions': len(decisions_df),
        'decisions_by_status': {},
        'avg_confidence_exported': 0.0,
        'review_completion_rate': 0.0
    }
    
    if len(review_pairs_df) > 0:
        stats['avg_confidence_exported'] = review_pairs_df['match_confidence'].mean()
    
    if len(decisions_df) > 0:
        stats['decisions_by_status'] = decisions_df['review_status'].value_counts().to_dict()
        stats['review_completion_rate'] = len(decisions_df) / len(review_pairs_df)
    
    logger.info(f"Review stats: {stats}")
    return stats


def create_review_summary(
    review_pairs_df: pd.DataFrame,
    decisions_df: pd.DataFrame,
    output_path: str = "data/review/review_summary.md"
) -> str:
    """
    Create a summary report of the review process.
    
    Args:
        review_pairs_df: Pairs exported for review
        decisions_df: Human review decisions
        output_path: Path to save summary report
        
    Returns:
        Summary report content
    """
    stats = get_review_stats(review_pairs_df, decisions_df)
    
    summary = f"""# Manual Review Summary

## Overview
- **Total pairs exported**: {stats['total_pairs_exported']}
- **Total decisions made**: {stats['total_decisions']}
- **Review completion rate**: {stats['review_completion_rate']:.1%}
- **Average confidence of exported pairs**: {stats['avg_confidence_exported']:.3f}

## Decision Breakdown
"""
    
    for status, count in stats['decisions_by_status'].items():
        summary += f"- **{status}**: {count}\n"
    
    summary += f"""
## Quality Metrics
- **Approval rate**: {stats['decisions_by_status'].get('approved', 0) / max(stats['total_decisions'], 1):.1%}
- **Rejection rate**: {stats['decisions_by_status'].get('rejected', 0) / max(stats['total_decisions'], 1):.1%}

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
    
    # Save summary
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(summary)
    
    logger.info(f"Review summary saved to {output_path}")
    return summary 
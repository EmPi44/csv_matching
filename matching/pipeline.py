"""
Main pipeline orchestration for Dubai Hills property matching.

Coordinates all three tiers of matching and generates final outputs.
"""

import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any, Tuple
from loguru import logger

from .preprocess import preprocess_owners, preprocess_transactions, get_data_info
from .deterministic import tier1_deterministic_match, validate_deterministic_matches
from .fuzzy import tier2_fuzzy_match, get_fuzzy_match_stats
from .review_helpers import (
    export_review_candidates, 
    load_review_decisions, 
    merge_review_decisions,
    create_review_summary
)


def run_matching_pipeline(
    owners_file: str,
    transactions_file: str,
    output_dir: str = "data/processed",
    review_dir: str = "data/review",
    run_id: str = None
) -> Dict[str, Any]:
    """
    Run the complete matching pipeline.
    
    Args:
        owners_file: Path to owners data file
        transactions_file: Path to transactions data file
        output_dir: Directory for pipeline outputs
        review_dir: Directory for review files
        run_id: Optional run ID (auto-generated if None)
        
    Returns:
        Dictionary with pipeline results and statistics
    """
    if run_id is None:
        run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    logger.info(f"Starting matching pipeline run: {run_id}")
    
    # Ensure output directories exist
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(review_dir, exist_ok=True)
    
    # Load raw data
    logger.info("Loading raw data")
    owners_df = load_data_file(owners_file)
    transactions_df = load_data_file(transactions_file)
    
    # Preprocess data
    logger.info("Preprocessing data")
    owners_clean = preprocess_owners(owners_df)
    transactions_clean = preprocess_transactions(transactions_df)
    
    # Tier 1: Deterministic matching
    logger.info("Running Tier 1: Deterministic matching")
    tier1_matches, unmatched_owners, unmatched_transactions = tier1_deterministic_match(
        owners_clean, transactions_clean
    )
    
    # Tier 2: Fuzzy matching
    logger.info("Running Tier 2: Fuzzy matching")
    tier2_matches, final_unmatched_owners, final_unmatched_transactions = tier2_fuzzy_match(
        unmatched_owners, unmatched_transactions
    )
    
    # Export candidates for manual review
    logger.info("Exporting candidates for manual review")
    review_pairs = export_review_candidates(
        tier2_matches, owners_clean, transactions_clean,
        os.path.join(review_dir, "pairs.parquet")
    )
    
    # Load and merge human review decisions
    logger.info("Loading human review decisions")
    review_decisions = load_review_decisions(review_dir=review_dir)
    
    if len(review_decisions) > 0:
        tier2_matches = merge_review_decisions(tier2_matches, review_decisions)
    
    # Combine all matches
    all_matches = pd.concat([tier1_matches, tier2_matches], ignore_index=True)
    
    # Write outputs
    logger.info("Writing pipeline outputs")
    write_pipeline_outputs(
        all_matches, final_unmatched_owners, final_unmatched_transactions,
        output_dir, run_id
    )
    
    # Generate QA report
    logger.info("Generating QA report")
    qa_report = generate_qa_report(
        all_matches, final_unmatched_owners, final_unmatched_transactions,
        owners_clean, transactions_clean, run_id, output_dir
    )
    
    # Create review summary if there were review pairs
    if len(review_pairs) > 0:
        create_review_summary(review_pairs, review_decisions, 
                            os.path.join(review_dir, "review_summary.md"))
    else:
        logger.info("No review pairs to create summary for")
    
    # Compile pipeline statistics
    pipeline_stats = compile_pipeline_stats(
        all_matches, tier1_matches, tier2_matches,
        final_unmatched_owners, final_unmatched_transactions,
        owners_clean, transactions_clean, run_id
    )
    
    logger.info(f"Pipeline run {run_id} completed successfully")
    return pipeline_stats


def load_data_file(file_path: str) -> pd.DataFrame:
    """
    Load data file (supports CSV, Excel, Parquet).
    
    Args:
        file_path: Path to data file
        
    Returns:
        Loaded DataFrame
    """
    logger.info(f"Loading data from: {file_path}")
    
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Data file not found: {file_path}")
    
    file_ext = os.path.splitext(file_path)[1].lower()
    
    if file_ext == '.csv':
        df = pd.read_csv(file_path)
    elif file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path)
    elif file_ext == '.parquet':
        df = pd.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")
    
    logger.info(f"Loaded {len(df)} records with {len(df.columns)} columns")
    return df


def write_pipeline_outputs(
    matches_df: pd.DataFrame,
    unmatched_owners_df: pd.DataFrame,
    unmatched_transactions_df: pd.DataFrame,
    output_dir: str,
    run_id: str
) -> None:
    """
    Write pipeline outputs to files.
    
    Args:
        matches_df: Final matches DataFrame
        unmatched_owners_df: Unmatched owners DataFrame
        unmatched_transactions_df: Unmatched transactions DataFrame
        output_dir: Output directory
        run_id: Pipeline run ID
    """
    # Write matches
    matches_path = os.path.join(output_dir, "matches.parquet")
    matches_df.to_parquet(matches_path, index=False)
    logger.info(f"Matches written to: {matches_path}")
    
    # Write unmatched owners
    if len(unmatched_owners_df) > 0:
        unmatched_owners_path = os.path.join(output_dir, "owners_unmatched.csv")
        unmatched_owners_df.to_csv(unmatched_owners_path, index=False)
        logger.info(f"Unmatched owners written to: {unmatched_owners_path}")
    
    # Write unmatched transactions
    if len(unmatched_transactions_df) > 0:
        unmatched_txns_path = os.path.join(output_dir, "transactions_unmatched.csv")
        unmatched_transactions_df.to_csv(unmatched_txns_path, index=False)
        logger.info(f"Unmatched transactions written to: {unmatched_txns_path}")


def generate_qa_report(
    matches_df: pd.DataFrame,
    unmatched_owners_df: pd.DataFrame,
    unmatched_transactions_df: pd.DataFrame,
    owners_clean_df: pd.DataFrame,
    transactions_clean_df: pd.DataFrame,
    run_id: str,
    output_dir: str
) -> str:
    """
    Generate QA report for the pipeline run.
    
    Args:
        matches_df: Final matches DataFrame
        unmatched_owners_df: Unmatched owners DataFrame
        unmatched_transactions_df: Unmatched transactions DataFrame
        owners_clean_df: Cleaned owners DataFrame
        transactions_clean_df: Cleaned transactions DataFrame
        run_id: Pipeline run ID
        output_dir: Output directory
        
    Returns:
        QA report content
    """
    total_owners = len(owners_clean_df)
    total_transactions = len(transactions_clean_df)
    total_matches = len(matches_df)
    
    # Calculate match rates
    owner_match_rate = total_matches / total_owners if total_owners > 0 else 0
    txn_match_rate = total_matches / total_transactions if total_transactions > 0 else 0
    
    # Confidence bucket distribution
    bucket_dist = matches_df['confidence_bucket'].value_counts().to_dict()
    
    # Score distribution
    score_ranges = {
        '0.95-1.00': ((matches_df['match_confidence'] >= 0.95) & (matches_df['match_confidence'] <= 1.0)).sum(),
        '0.90-0.95': ((matches_df['match_confidence'] >= 0.90) & (matches_df['match_confidence'] < 0.95)).sum(),
        '0.85-0.90': ((matches_df['match_confidence'] >= 0.85) & (matches_df['match_confidence'] < 0.90)).sum(),
        '0.75-0.85': ((matches_df['match_confidence'] >= 0.75) & (matches_df['match_confidence'] < 0.85)).sum()
    }
    
    report = f"""# Pipeline QA Report

## Run Information
- **Run ID**: {run_id}
- **Generated**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Data Summary
- **Total Owners**: {total_owners:,}
- **Total Transactions**: {total_transactions:,}
- **Total Matches**: {total_matches:,}

## Match Rates
- **Owner Match Rate**: {owner_match_rate:.1%}
- **Transaction Match Rate**: {txn_match_rate:.1%}

## Confidence Distribution
"""
    
    for bucket, count in bucket_dist.items():
        report += f"- **{bucket}**: {count:,} ({count/total_matches:.1%})\n"
    
    report += f"""
## Score Distribution
"""
    
    for range_name, count in score_ranges.items():
        report += f"- **{range_name}**: {count:,} ({count/total_matches:.1%})\n"
    
    report += f"""
## Unmatched Records
- **Unmatched Owners**: {len(unmatched_owners_df):,}
- **Unmatched Transactions**: {len(unmatched_transactions_df):,}

## Quality Metrics
- **Average Match Confidence**: {matches_df['match_confidence'].mean():.3f}
- **High Confidence Rate**: {bucket_dist.get('High', 0) / total_matches:.1%}
- **Precision Estimate**: {bucket_dist.get('High', 0) / total_matches:.1%}
"""
    
    # Save report
    report_path = os.path.join(output_dir, "qa_report.md")
    with open(report_path, 'w') as f:
        f.write(report)
    
    logger.info(f"QA report written to: {report_path}")
    return report


def compile_pipeline_stats(
    all_matches_df: pd.DataFrame,
    tier1_matches_df: pd.DataFrame,
    tier2_matches_df: pd.DataFrame,
    unmatched_owners_df: pd.DataFrame,
    unmatched_transactions_df: pd.DataFrame,
    owners_clean_df: pd.DataFrame,
    transactions_clean_df: pd.DataFrame,
    run_id: str
) -> Dict[str, Any]:
    """
    Compile comprehensive pipeline statistics.
    
    Args:
        all_matches_df: All matches from pipeline
        tier1_matches_df: Tier 1 matches
        tier2_matches_df: Tier 2 matches
        unmatched_owners_df: Unmatched owners
        unmatched_transactions_df: Unmatched transactions
        owners_clean_df: Cleaned owners
        transactions_clean_df: Cleaned transactions
        run_id: Pipeline run ID
        
    Returns:
        Dictionary with pipeline statistics
    """
    stats = {
        'run_id': run_id,
        'timestamp': datetime.now().isoformat(),
        'data_volumes': {
            'total_owners': len(owners_clean_df),
            'total_transactions': len(transactions_clean_df),
            'total_matches': len(all_matches_df),
            'unmatched_owners': len(unmatched_owners_df),
            'unmatched_transactions': len(unmatched_transactions_df)
        },
        'match_rates': {
            'owner_match_rate': len(all_matches_df) / len(owners_clean_df) if len(owners_clean_df) > 0 else 0,
            'transaction_match_rate': len(all_matches_df) / len(transactions_clean_df) if len(transactions_clean_df) > 0 else 0
        },
        'tier_performance': {
            'tier1_matches': len(tier1_matches_df),
            'tier2_matches': len(tier2_matches_df),
            'tier1_rate': len(tier1_matches_df) / len(owners_clean_df) if len(owners_clean_df) > 0 else 0,
            'tier2_rate': len(tier2_matches_df) / len(owners_clean_df) if len(owners_clean_df) > 0 else 0
        },
        'confidence_distribution': all_matches_df['confidence_bucket'].value_counts().to_dict(),
        'quality_metrics': {
            'avg_confidence': all_matches_df['match_confidence'].mean(),
            'high_confidence_rate': (all_matches_df['confidence_bucket'] == 'High').mean(),
            'precision_estimate': (all_matches_df['confidence_bucket'] == 'High').mean()
        }
    }
    
    logger.info(f"Pipeline statistics compiled: {stats}")
    return stats 
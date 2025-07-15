"""
Streamlit review application for manual review of property matches.

This app allows reviewers to approve or reject low-confidence matches
from the Dubai Hills property matching pipeline.
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from typing import Dict, Any


def load_review_pairs(file_path: str = "data/review/pairs.parquet") -> pd.DataFrame:
    """
    Load pairs for review.
    
    Args:
        file_path: Path to review pairs file
        
    Returns:
        DataFrame with pairs for review
    """
    if not os.path.exists(file_path):
        st.error(f"Review pairs file not found: {file_path}")
        return pd.DataFrame()
    
    try:
        df = pd.read_parquet(file_path)
        st.success(f"Loaded {len(df)} pairs for review")
        return df
    except Exception as e:
        st.error(f"Error loading review pairs: {e}")
        return pd.DataFrame()


def save_review_decisions(decisions: list, output_dir: str = "data/review"):
    """
    Save review decisions to file.
    
    Args:
        decisions: List of decision dictionaries
        output_dir: Output directory for decisions
    """
    if not decisions:
        st.warning("No decisions to save")
        return
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Convert to DataFrame
    decisions_df = pd.DataFrame(decisions)
    
    # Add timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    decisions_df['review_timestamp'] = datetime.now().isoformat()
    decisions_df['reviewer'] = st.session_state.get('reviewer_name', 'Unknown')
    
    # Save to file
    output_file = os.path.join(output_dir, f"decisions_{timestamp}.parquet")
    decisions_df.to_parquet(output_file, index=False)
    
    st.success(f"Saved {len(decisions)} decisions to {output_file}")


def display_pair_comparison(pair: pd.Series) -> None:
    """
    Display a pair comparison for review.
    
    Args:
        pair: Series containing pair data
    """
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Owner Record")
        st.write(f"**Project**: {pair.get('project_clean_owner', 'N/A')}")
        st.write(f"**Building**: {pair.get('building_clean_owner', 'N/A')}")
        st.write(f"**Unit**: {pair.get('unit_no_owner', 'N/A')}")
        st.write(f"**Area**: {pair.get('area_sqm_owner', 'N/A')} sqm")
        st.write(f"**Owner Name**: {pair.get('owner_name_clean', 'N/A')}")
    
    with col2:
        st.subheader("Transaction Record")
        st.write(f"**Project**: {pair.get('project_clean_txn', 'N/A')}")
        st.write(f"**Building**: {pair.get('building_clean_txn', 'N/A')}")
        st.write(f"**Unit**: {pair.get('unit_no_txn', 'N/A')}")
        st.write(f"**Area**: {pair.get('area_sqm_txn', 'N/A')} sqm")
        st.write(f"**Buyer Name**: {pair.get('buyer_name_clean', 'N/A')}")
    
    # Show matching score
    st.metric(
        label="Match Confidence",
        value=f"{pair.get('match_confidence', 0):.3f}",
        delta=f"Bucket: {pair.get('confidence_bucket', 'Unknown')}"
    )


def main():
    """Main Streamlit application."""
    st.set_page_config(
        page_title="Dubai Hills Property Matching Review",
        page_icon="🏠",
        layout="wide"
    )
    
    st.title("🏠 Dubai Hills Property Matching Review")
    st.markdown("Review low-confidence property matches from the matching pipeline.")
    
    # Initialize session state
    if 'reviewer_name' not in st.session_state:
        st.session_state.reviewer_name = ""
    if 'current_index' not in st.session_state:
        st.session_state.current_index = 0
    if 'decisions' not in st.session_state:
        st.session_state.decisions = []
    if 'review_pairs' not in st.session_state:
        st.session_state.review_pairs = pd.DataFrame()
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Reviewer name
        reviewer_name = st.text_input(
            "Reviewer Name",
            value=st.session_state.reviewer_name,
            help="Enter your name for tracking purposes"
        )
        st.session_state.reviewer_name = reviewer_name
        
        # Load review pairs
        pairs_file = st.text_input(
            "Review Pairs File",
            value="data/review/pairs.parquet",
            help="Path to the review pairs file"
        )
        
        if st.button("Load Review Pairs"):
            st.session_state.review_pairs = load_review_pairs(pairs_file)
            st.session_state.current_index = 0
            st.session_state.decisions = []
            st.rerun()
        
        # Show progress
        if len(st.session_state.review_pairs) > 0:
            st.header("Progress")
            total_pairs = len(st.session_state.review_pairs)
            reviewed = len(st.session_state.decisions)
            progress = reviewed / total_pairs if total_pairs > 0 else 0
            
            st.progress(progress)
            st.write(f"Reviewed: {reviewed} / {total_pairs}")
            st.write(f"Remaining: {total_pairs - reviewed}")
            
            # Save decisions button
            if st.button("Save Decisions"):
                save_review_decisions(st.session_state.decisions)
    
    # Main content area
    if len(st.session_state.review_pairs) == 0:
        st.info("Please load review pairs using the sidebar.")
        return
    
    # Get current pair
    current_pair = st.session_state.review_pairs.iloc[st.session_state.current_index]
    
    # Display pair information
    st.header(f"Pair {st.session_state.current_index + 1} of {len(st.session_state.review_pairs)}")
    
    display_pair_comparison(current_pair)
    
    # Review controls
    st.header("Review Decision")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("✅ Approve", type="primary"):
            decision = {
                'owner_id': current_pair['owner_id'],
                'txn_id': current_pair['txn_id'],
                'review_status': 'approved',
                'reviewer_notes': '',
                'original_confidence': current_pair['match_confidence']
            }
            st.session_state.decisions.append(decision)
            st.session_state.current_index += 1
            st.rerun()
    
    with col2:
        if st.button("❌ Reject", type="secondary"):
            decision = {
                'owner_id': current_pair['owner_id'],
                'txn_id': current_pair['txn_id'],
                'review_status': 'rejected',
                'reviewer_notes': '',
                'original_confidence': current_pair['match_confidence']
            }
            st.session_state.decisions.append(decision)
            st.session_state.current_index += 1
            st.rerun()
    
    with col3:
        if st.button("⏭️ Skip"):
            st.session_state.current_index += 1
            st.rerun()
    
    # Navigation controls
    st.header("Navigation")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("⏮️ First"):
            st.session_state.current_index = 0
            st.rerun()
    
    with col2:
        if st.button("◀️ Previous") and st.session_state.current_index > 0:
            st.session_state.current_index -= 1
            st.rerun()
    
    with col3:
        if st.button("▶️ Next") and st.session_state.current_index < len(st.session_state.review_pairs) - 1:
            st.session_state.current_index += 1
            st.rerun()
    
    with col4:
        if st.button("⏭️ Last"):
            st.session_state.current_index = len(st.session_state.review_pairs) - 1
            st.rerun()
    
    # Show recent decisions
    if st.session_state.decisions:
        st.header("Recent Decisions")
        
        recent_decisions = pd.DataFrame(st.session_state.decisions[-10:])  # Show last 10
        st.dataframe(recent_decisions, use_container_width=True)


if __name__ == "__main__":
    main() 
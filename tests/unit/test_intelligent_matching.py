#!/usr/bin/env python3
"""
Test script for intelligent fuzzy matching on Dubai Hills property records.
"""

import pandas as pd
import logging
import sys
import os
from datetime import datetime

# Add the project root to the path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from matching.intelligent_fuzzy_matcher import IntelligentFuzzyMatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def load_data():
    """Load owners and transactions data."""
    logger.info("Loading data...")
    
    # Load owners data
    owners_file = "data/raw/owners/20250716/Dubai Hills.csv"
    owners_df = pd.read_csv(owners_file)
    logger.info(f"Loaded {len(owners_df)} owner records")
    
    # Load a small transaction file for testing
    transactions_file = "data/raw/transactions/20250716/split_analysis/part_aa.csv"
    transactions_df = pd.read_csv(transactions_file)
    logger.info(f"Loaded {len(transactions_df)} transaction records")
    
    return owners_df, transactions_df

def analyze_data_structure(owners_df, transactions_df):
    """Analyze the data structure to understand matching fields."""
    logger.info("Analyzing data structure...")
    
    print("\n=== OWNERS DATA STRUCTURE ===")
    print(f"Columns: {list(owners_df.columns)}")
    print(f"Sample records:")
    print(owners_df.head(3).to_string())
    
    print("\n=== TRANSACTIONS DATA STRUCTURE ===")
    print(f"Columns: {list(transactions_df.columns)}")
    print(f"Sample records:")
    print(transactions_df.head(3).to_string())
    
    # Check for Dubai Hills related records
    print("\n=== DUBAI HILLS RECORDS ANALYSIS ===")
    
    # Check owners for Dubai Hills projects
    dubai_hills_owners = owners_df[owners_df['Project'].str.contains('Dubai Hills|Sidra|Collective|Park', case=False, na=False)]
    print(f"Owners with Dubai Hills related projects: {len(dubai_hills_owners)}")
    
    # Check transactions for Dubai Hills
    dubai_hills_transactions = transactions_df[
        transactions_df['project_name_en'].str.contains('Dubai Hills|Sidra|Collective|Park', case=False, na=False) |
        transactions_df['master_project_en'].str.contains('Dubai Hills|Sidra|Collective|Park', case=False, na=False) |
        transactions_df['area_name_en'].str.contains('Dubai Hills|Hadaeq', case=False, na=False)
    ]
    print(f"Transactions with Dubai Hills related projects: {len(dubai_hills_transactions)}")
    
    return dubai_hills_owners, dubai_hills_transactions

def test_intelligent_matching(owners_df, transactions_df):
    """Test the intelligent fuzzy matching."""
    logger.info("Testing intelligent fuzzy matching...")
    
    # Create matcher with different confidence thresholds
    matcher_high = IntelligentFuzzyMatcher(confidence_threshold=0.8)
    matcher_medium = IntelligentFuzzyMatcher(confidence_threshold=0.7)
    matcher_low = IntelligentFuzzyMatcher(confidence_threshold=0.6)
    
    print("\n=== INTELLIGENT FUZZY MATCHING RESULTS ===")
    
    # Test with high confidence
    print(f"\n--- High Confidence Matches (>= 0.8) ---")
    high_matches = matcher_high.find_matches(owners_df, transactions_df)
    high_summary = matcher_high.get_match_summary(high_matches)
    print(f"Total matches: {high_summary['total_matches']}")
    print(f"Match types: {high_summary['match_types']}")
    print(f"Average confidence: {high_summary['average_confidence']:.3f}")
    
    # Test with medium confidence
    print(f"\n--- Medium Confidence Matches (>= 0.7) ---")
    medium_matches = matcher_medium.find_matches(owners_df, transactions_df)
    medium_summary = matcher_medium.get_match_summary(medium_matches)
    print(f"Total matches: {medium_summary['total_matches']}")
    print(f"Match types: {medium_summary['match_types']}")
    print(f"Average confidence: {medium_summary['average_confidence']:.3f}")
    
    # Test with low confidence
    print(f"\n--- Low Confidence Matches (>= 0.6) ---")
    low_matches = matcher_low.find_matches(owners_df, transactions_df)
    low_summary = matcher_low.get_match_summary(low_matches)
    print(f"Total matches: {low_summary['total_matches']}")
    print(f"Match types: {low_summary['match_types']}")
    print(f"Average confidence: {low_summary['average_confidence']:.3f}")
    
    return high_matches, medium_matches, low_matches

def show_detailed_matches(matches, title):
    """Show detailed information about matches."""
    if not matches:
        print(f"\n{title}: No matches found")
        return
    
    print(f"\n{title}: {len(matches)} matches")
    print("=" * 80)
    
    for i, match in enumerate(matches[:10]):  # Show first 10 matches
        print(f"\nMatch {i+1}:")
        print(f"  Confidence: {match.confidence_score:.3f}")
        print(f"  Type: {match.match_type}")
        print(f"  Matching fields: {match.matching_fields}")
        
        # Show owner details
        owner = match.owner_record
        print(f"  Owner: {owner.get('NameEn', 'N/A')} - {owner.get('Project', 'N/A')} - {owner.get('BuildingNameEn', 'N/A')}")
        print(f"    Unit: {owner.get('UnitNumber', 'N/A')} - Value: {owner.get('ProcedureValue', 'N/A')}")
        
        # Show transaction details
        transaction = match.transaction_record
        print(f"  Transaction: {transaction.get('project_name_en', 'N/A')} - {transaction.get('building_name_en', 'N/A')}")
        print(f"    Area: {transaction.get('procedure_area', 'N/A')} - Value: {transaction.get('actual_worth', 'N/A')}")

def test_specific_matching_scenarios():
    """Test specific matching scenarios."""
    logger.info("Testing specific matching scenarios...")
    
    matcher = IntelligentFuzzyMatcher(confidence_threshold=0.5)
    
    print("\n=== SPECIFIC MATCHING SCENARIOS ===")
    
    # Test project name matching
    test_cases = [
        ("Prive Residence", "PRIVE RESIDENCE"),
        ("Sidra 3", "SIDRA"),
        ("Collective 2.0", "COLLECTIVE 2.0 TOWER A"),
        ("Park Ridge", "PARK RIDGE TOWER C"),
        ("Dubai Hills Estate", "DUBAI HILLS ESTATE")
    ]
    
    print("\n--- Project Name Matching Tests ---")
    for owner_project, transaction_project in test_cases:
        similarity = matcher.match_project_names(owner_project, transaction_project)
        print(f"'{owner_project}' vs '{transaction_project}': {similarity:.3f}")
    
    # Test value matching
    print("\n--- Value Matching Tests ---")
    value_tests = [
        (890000, 900000),
        (1220000, 1200000),
        (3800000, 4000000),
        (1250000, 1300000)
    ]
    
    for owner_val, transaction_val in value_tests:
        similarity = matcher.match_values(owner_val, transaction_val)
        print(f"{owner_val:,} vs {transaction_val:,}: {similarity:.3f}")

def main():
    """Main function to run the intelligent matching test."""
    start_time = datetime.now()
    logger.info("Starting intelligent fuzzy matching test")
    
    try:
        # Load data
        owners_df, transactions_df = load_data()
        
        # Analyze data structure
        dubai_hills_owners, dubai_hills_transactions = analyze_data_structure(owners_df, transactions_df)
        
        # Test specific scenarios
        test_specific_matching_scenarios()
        
        # Test intelligent matching
        high_matches, medium_matches, low_matches = test_intelligent_matching(owners_df, transactions_df)
        
        # Show detailed matches
        show_detailed_matches(high_matches, "HIGH CONFIDENCE MATCHES")
        show_detailed_matches(medium_matches, "MEDIUM CONFIDENCE MATCHES")
        show_detailed_matches(low_matches, "LOW CONFIDENCE MATCHES")
        
        # Summary
        duration = datetime.now() - start_time
        logger.info(f"Intelligent matching test completed in {duration}")
        
        print(f"\n=== SUMMARY ===")
        print(f"Test completed in {duration}")
        print(f"High confidence matches: {len(high_matches)}")
        print(f"Medium confidence matches: {len(medium_matches)}")
        print(f"Low confidence matches: {len(low_matches)}")
        
    except Exception as e:
        logger.error(f"Error during intelligent matching test: {e}")
        raise

if __name__ == "__main__":
    main() 
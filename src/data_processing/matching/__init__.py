"""
Dubai Hills Property Owner ↔ Transaction Matching Pipeline

This module provides a three-tier matching system to link property owner records
to DLD transaction records with high precision and recall.
"""

__version__ = "1.0.0"
__author__ = "Dubai Hills Matching Team"

from .preprocess import preprocess_owners, preprocess_transactions
from .deterministic import tier1_deterministic_match
from .fuzzy import tier2_fuzzy_match
from .pipeline import run_matching_pipeline

__all__ = [
    "preprocess_owners",
    "preprocess_transactions", 
    "tier1_deterministic_match",
    "tier2_fuzzy_match",
    "run_matching_pipeline"
] 
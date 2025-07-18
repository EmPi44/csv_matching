"""
Intelligent Fuzzy Matcher for Dubai Hills Property Records

This module implements sophisticated fuzzy matching algorithms specifically designed
for matching Dubai Hills property owner records with DLD transaction records.
"""

import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from fuzzywuzzy import fuzz
import re
import logging
from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class MatchResult:
    """Represents a match between owner and transaction records"""
    owner_index: int
    transaction_index: int
    confidence_score: float
    match_type: str
    matching_fields: Dict[str, float]
    owner_record: pd.Series
    transaction_record: pd.Series

class IntelligentFuzzyMatcher:
    """
    Intelligent fuzzy matcher for Dubai Hills property records.
    
    Implements multi-stage matching with confidence scoring:
    1. Exact property matching
    2. Fuzzy property matching
    3. Owner name matching
    """
    
    def __init__(self, confidence_threshold: float = 0.7):
        self.confidence_threshold = confidence_threshold
        
        # Text normalization patterns
        self.normalization_patterns = {
            'llc_variations': [r'\bLLC\b', r'\bL\.L\.C\b', r'\bL\.L\.C\.\b'],
            'common_abbreviations': {
                'building': 'bldg',
                'tower': 'twr',
                'residence': 'res',
                'apartment': 'apt',
                'street': 'st',
                'avenue': 'ave',
                'road': 'rd'
            }
        }
        
        # Project name mappings for Dubai Hills
        self.project_mappings = {
            'prive residence': ['prive', 'prive residence'],
            'sidra': ['sidra', 'sidra 3', 'sidra 1', 'sidra 2'],
            'collective': ['collective', 'collective 2.0', 'collective 2.0 tower'],
            'park ridge': ['park ridge', 'park ridge tower'],
            'park heights': ['park heights', 'acacia at park heights', 'mulberry at park heights'],
            'socio': ['socio', 'socio tower']
        }
    
    def normalize_text(self, text: str) -> str:
        """
        Normalize text for comparison by removing special characters,
        converting to lowercase, and standardizing common patterns.
        """
        if pd.isna(text) or text == '':
            return ''
        
        # Convert to string and lowercase
        text = str(text).lower().strip()
        
        # Remove special characters but keep spaces and numbers
        text = re.sub(r'[^\w\s\d]', ' ', text)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)
        
        # Handle common abbreviations
        for full, abbrev in self.normalization_patterns['common_abbreviations'].items():
            text = re.sub(rf'\b{full}\b', abbrev, text)
        
        return text.strip()
    
    def calculate_text_similarity(self, text1: str, text2: str) -> float:
        """
        Calculate similarity between two text strings using multiple algorithms.
        """
        if not text1 or not text2:
            return 0.0
        
        norm1 = self.normalize_text(text1)
        norm2 = self.normalize_text(text2)
        
        if not norm1 or not norm2:
            return 0.0
        
        # Use multiple similarity metrics
        ratios = [
            fuzz.ratio(norm1, norm2) / 100.0,
            fuzz.partial_ratio(norm1, norm2) / 100.0,
            fuzz.token_sort_ratio(norm1, norm2) / 100.0,
            fuzz.token_set_ratio(norm1, norm2) / 100.0
        ]
        
        # Return weighted average
        return np.mean(ratios)
    
    def match_project_names(self, owner_project: str, transaction_project: str) -> float:
        """
        Match project names with special handling for Dubai Hills projects.
        """
        if pd.isna(owner_project) or pd.isna(transaction_project):
            return 0.0
        
        owner_norm = self.normalize_text(owner_project)
        transaction_norm = self.normalize_text(transaction_project)
        
        # Check for exact match first
        if owner_norm == transaction_norm:
            return 1.0
        
        # Check project mappings
        for key, variations in self.project_mappings.items():
            if owner_norm in variations and transaction_norm in variations:
                return 0.95
            elif owner_norm in variations or transaction_norm in variations:
                # Check if one is a variation of the other
                for variation in variations:
                    if variation in owner_norm or variation in transaction_norm:
                        return 0.85
        
        # Use general text similarity
        return self.calculate_text_similarity(owner_project, transaction_project)
    
    def match_building_names(self, owner_building: str, transaction_building: str) -> float:
        """
        Match building names with unit number consideration.
        """
        if pd.isna(owner_building) or pd.isna(transaction_building):
            return 0.0
        
        owner_norm = self.normalize_text(owner_building)
        transaction_norm = self.normalize_text(transaction_building)
        
        # Check for exact match
        if owner_norm == transaction_norm:
            return 1.0
        
        # Check if one contains the other
        if owner_norm in transaction_norm or transaction_norm in owner_norm:
            return 0.9
        
        # Use general similarity
        return self.calculate_text_similarity(owner_building, transaction_building)
    
    def match_values(self, owner_value: float, transaction_value: float, tolerance: float = 0.15) -> float:
        """
        Match transaction values with tolerance.
        """
        if pd.isna(owner_value) or pd.isna(transaction_value):
            return 0.0
        
        try:
            owner_val = float(owner_value)
            transaction_val = float(transaction_value)
            
            if owner_val == 0 or transaction_val == 0:
                return 0.0
            
            # Calculate percentage difference
            diff = abs(owner_val - transaction_val) / max(owner_val, transaction_val)
            
            if diff <= tolerance:
                # Higher score for closer matches
                return max(0.7, 1.0 - (diff / tolerance) * 0.3)
            else:
                return 0.0
                
        except (ValueError, TypeError):
            return 0.0
    
    def match_areas(self, owner_area: str, transaction_area: float, tolerance: float = 0.10) -> float:
        """
        Match area sizes with tolerance.
        """
        if pd.isna(owner_area) or pd.isna(transaction_area):
            return 0.0
        
        try:
            # Extract numeric value from owner area
            owner_area_str = str(owner_area)
            owner_area_match = re.search(r'(\d+\.?\d*)', owner_area_str)
            
            if not owner_area_match:
                return 0.0
            
            owner_area_val = float(owner_area_match.group(1))
            transaction_area_val = float(transaction_area)
            
            if owner_area_val == 0 or transaction_area_val == 0:
                return 0.0
            
            # Calculate percentage difference
            diff = abs(owner_area_val - transaction_area_val) / max(owner_area_val, transaction_area_val)
            
            if diff <= tolerance:
                return max(0.7, 1.0 - (diff / tolerance) * 0.3)
            else:
                return 0.0
                
        except (ValueError, TypeError):
            return 0.0
    
    def match_geographic_areas(self, owner_area: str, transaction_area: str) -> float:
        """
        Match geographic area names.
        """
        if pd.isna(owner_area) or pd.isna(transaction_area):
            return 0.0
        
        # Dubai Hills specific area mappings
        dubai_hills_areas = [
            'hadaeq sheikh mohammed bin rashid',
            'dubai hills estate',
            'dubai hills',
            'motor city'
        ]
        
        owner_norm = self.normalize_text(owner_area)
        transaction_norm = self.normalize_text(transaction_area)
        
        # Check if both are in Dubai Hills areas
        owner_in_dubai_hills = any(area in owner_norm for area in dubai_hills_areas)
        transaction_in_dubai_hills = any(area in transaction_norm for area in dubai_hills_areas)
        
        if owner_in_dubai_hills and transaction_in_dubai_hills:
            return 0.8
        
        # Use general similarity
        return self.calculate_text_similarity(owner_area, transaction_area)
    
    def calculate_match_confidence(self, matching_fields: Dict[str, float]) -> float:
        """
        Calculate overall confidence score based on individual field matches.
        """
        if not matching_fields:
            return 0.0
        
        # Weight different fields by importance
        field_weights = {
            'project_name': 0.25,
            'building_name': 0.20,
            'value': 0.20,
            'area': 0.15,
            'geographic_area': 0.10,
            'unit_number': 0.10
        }
        
        total_score = 0.0
        total_weight = 0.0
        
        for field, score in matching_fields.items():
            weight = field_weights.get(field, 0.05)
            total_score += score * weight
            total_weight += weight
        
        if total_weight == 0:
            return 0.0
        
        return total_score / total_weight
    
    def find_matches(self, owners_df: pd.DataFrame, transactions_df: pd.DataFrame) -> List[MatchResult]:
        """
        Find matches between owners and transactions using intelligent fuzzy matching.
        """
        matches = []
        
        logger.info(f"Starting intelligent fuzzy matching for {len(owners_df)} owners and {len(transactions_df)} transactions")
        
        for owner_idx, owner_row in owners_df.iterrows():
            for transaction_idx, transaction_row in transactions_df.iterrows():
                
                # Calculate individual field similarities
                matching_fields = {}
                
                # Project name matching
                project_similarity = self.match_project_names(
                    owner_row.get('Project', ''),
                    transaction_row.get('project_name_en', '')
                )
                if project_similarity > 0:
                    matching_fields['project_name'] = project_similarity
                
                # Building name matching
                building_similarity = self.match_building_names(
                    owner_row.get('BuildingNameEn', ''),
                    transaction_row.get('building_name_en', '')
                )
                if building_similarity > 0:
                    matching_fields['building_name'] = building_similarity
                
                # Value matching
                value_similarity = self.match_values(
                    owner_row.get('ProcedureValue', 0),
                    transaction_row.get('actual_worth', 0)
                )
                if value_similarity > 0:
                    matching_fields['value'] = value_similarity
                
                # Area matching
                area_similarity = self.match_areas(
                    owner_row.get(' Size ', ''),
                    transaction_row.get('procedure_area', 0)
                )
                if area_similarity > 0:
                    matching_fields['area'] = area_similarity
                
                # Geographic area matching
                geographic_similarity = self.match_geographic_areas(
                    owner_row.get('Project', ''),
                    transaction_row.get('area_name_en', '')
                )
                if geographic_similarity > 0:
                    matching_fields['geographic_area'] = geographic_similarity
                
                # Calculate overall confidence
                confidence = self.calculate_match_confidence(matching_fields)
                
                # Only include matches above threshold
                if confidence >= self.confidence_threshold:
                    match_type = self._determine_match_type(matching_fields)
                    
                    match_result = MatchResult(
                        owner_index=owner_idx,
                        transaction_index=transaction_idx,
                        confidence_score=confidence,
                        match_type=match_type,
                        matching_fields=matching_fields,
                        owner_record=owner_row,
                        transaction_record=transaction_row
                    )
                    
                    matches.append(match_result)
        
        # Sort by confidence score (highest first)
        matches.sort(key=lambda x: x.confidence_score, reverse=True)
        
        logger.info(f"Found {len(matches)} matches with confidence >= {self.confidence_threshold}")
        
        return matches
    
    def _determine_match_type(self, matching_fields: Dict[str, float]) -> str:
        """
        Determine the type of match based on matching fields.
        """
        if matching_fields.get('project_name', 0) > 0.9 and matching_fields.get('building_name', 0) > 0.9:
            return 'exact_property'
        elif matching_fields.get('project_name', 0) > 0.7 and matching_fields.get('value', 0) > 0.7:
            return 'fuzzy_property_value'
        elif matching_fields.get('building_name', 0) > 0.7 and matching_fields.get('area', 0) > 0.7:
            return 'fuzzy_building_area'
        elif matching_fields.get('geographic_area', 0) > 0.7 and matching_fields.get('value', 0) > 0.7:
            return 'geographic_value'
        else:
            return 'fuzzy_general'
    
    def get_match_summary(self, matches: List[MatchResult]) -> Dict:
        """
        Generate summary statistics for matches.
        """
        if not matches:
            return {
                'total_matches': 0,
                'match_types': {},
                'confidence_ranges': {},
                'average_confidence': 0.0
            }
        
        match_types = {}
        confidence_ranges = {
            'high': 0,    # 0.9-1.0
            'medium': 0,  # 0.7-0.9
            'low': 0      # 0.7-0.8
        }
        
        for match in matches:
            # Count match types
            match_types[match.match_type] = match_types.get(match.match_type, 0) + 1
            
            # Count confidence ranges
            if match.confidence_score >= 0.9:
                confidence_ranges['high'] += 1
            elif match.confidence_score >= 0.8:
                confidence_ranges['medium'] += 1
            else:
                confidence_ranges['low'] += 1
        
        return {
            'total_matches': len(matches),
            'match_types': match_types,
            'confidence_ranges': confidence_ranges,
            'average_confidence': np.mean([m.confidence_score for m in matches])
        } 
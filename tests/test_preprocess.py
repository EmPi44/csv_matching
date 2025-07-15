"""
Unit tests for preprocessing module.
"""

import pytest
import pandas as pd
import sys
import os

# Add the parent directory to the path so we can import the matching module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from matching.preprocess import (
    normalize_string, 
    replace_synonyms, 
    extract_unit_number, 
    normalize_area,
    generate_composite_key
)


class TestNormalizeString:
    """Test string normalization functions."""
    
    def test_normalize_string_basic(self):
        """Test basic string normalization."""
        assert normalize_string("  HELLO World  ") == "hello world"
        assert normalize_string("") == ""
        assert normalize_string(None) == ""
        assert normalize_string(pd.NA) == ""
    
    def test_normalize_string_unicode(self):
        """Test Unicode normalization."""
        # Test with some Unicode characters
        assert normalize_string("Café") == "café"
        assert normalize_string("Dubai Hills") == "dubai hills"


class TestReplaceSynonyms:
    """Test synonym replacement."""
    
    def test_building_synonyms(self):
        """Test building/tower synonym replacement."""
        assert replace_synonyms("Tower A") == "tower a"
        assert replace_synonyms("Building 1") == "tower 1"
        assert replace_synonyms("BLDG 3") == "tower 3"
        assert replace_synonyms("Block 5") == "tower 5"
    
    def test_roman_numerals(self):
        """Test Roman numeral conversion."""
        assert replace_synonyms("Tower I") == "tower 1"
        assert replace_synonyms("Building II") == "tower 2"
        assert replace_synonyms("BLDG III") == "tower 3"
        assert replace_synonyms("Block X") == "tower 10"


class TestExtractUnitNumber:
    """Test unit number extraction."""
    
    def test_extract_unit_number_basic(self):
        """Test basic unit number extraction."""
        assert extract_unit_number("Apt 101") == "0101"
        assert extract_unit_number("Unit 5") == "0005"
        assert extract_unit_number("123") == "0123"
        assert extract_unit_number("") == ""
        assert extract_unit_number(None) == ""
    
    def test_extract_unit_number_padding(self):
        """Test zero padding of unit numbers."""
        assert extract_unit_number("1") == "0001"
        assert extract_unit_number("12") == "0012"
        assert extract_unit_number("123") == "0123"
        assert extract_unit_number("1234") == "1234"


class TestNormalizeArea:
    """Test area normalization."""
    
    def test_normalize_area_basic(self):
        """Test basic area normalization."""
        assert normalize_area(100.5) == 100.5
        assert normalize_area("150.25") == 150.25
        assert normalize_area(None) == 0.0
        assert normalize_area("") == 0.0
    
    def test_normalize_area_sqft_conversion(self):
        """Test square feet to square meters conversion."""
        # 1000 sqft should be converted to sqm
        sqft_area = 1000
        expected_sqm = round(sqft_area * 0.092903, 2)
        assert normalize_area(sqft_area) == expected_sqm
        
        # Large areas should not be converted
        large_area = 5000
        assert normalize_area(large_area) == 5000.0


class TestGenerateCompositeKey:
    """Test composite key generation."""
    
    def test_generate_composite_key_apartment(self):
        """Test composite key generation for apartments."""
        row = pd.Series({
            'project_clean': 'dubai hills',
            'building_clean': 'tower 1',
            'unit_no': '0101',
            'plot_no': ''
        })
        
        key = generate_composite_key(row, "apartment")
        assert key == "dubai_hills_tower_1_0101"
    
    def test_generate_composite_key_villa(self):
        """Test composite key generation for villas."""
        row = pd.Series({
            'project_clean': 'dubai hills',
            'building_clean': '',
            'unit_no': '',
            'plot_no': '0015'
        })
        
        key = generate_composite_key(row, "villa")
        assert key == "dubai_hills_0015"


if __name__ == "__main__":
    pytest.main([__file__]) 
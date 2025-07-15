"""
Data preprocessing for Dubai Hills property matching pipeline.

Handles cleaning, normalization, and composite key generation for both
owner and transaction datasets.
"""

import pandas as pd
import re
import unicodedata
from typing import Tuple, Dict, Any
from loguru import logger


def normalize_string(text: str) -> str:
    """
    Normalize string fields: lowercase, strip whitespace, normalize Unicode.
    
    Args:
        text: Input string to normalize
        
    Returns:
        Normalized string
    """
    if pd.isna(text):
        return ""
    
    # Convert to string and normalize Unicode
    text = str(text)
    text = unicodedata.normalize('NFKC', text)
    
    # Lowercase and strip whitespace
    text = text.lower().strip()
    
    return text


def replace_synonyms(text: str) -> str:
    """
    Replace common synonyms and abbreviations.
    
    Args:
        text: Input text
        
    Returns:
        Text with synonyms replaced
    """
    if not text:
        return text
    
    # Building/tower synonyms
    synonyms = {
        r'\btower\b': 'tower',
        r'\bbldg\b': 'tower', 
        r'\bbuilding\b': 'tower',
        r'\bblk\b': 'tower',
        r'\bblock\b': 'tower',
    }
    
    for pattern, replacement in synonyms.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    
    # Roman numerals to digits (basic implementation)
    roman_to_digit = {
        'i': '1', 'ii': '2', 'iii': '3', 'iv': '4', 'v': '5',
        'vi': '6', 'vii': '7', 'viii': '8', 'ix': '9', 'x': '10'
    }
    
    for roman, digit in roman_to_digit.items():
        text = re.sub(rf'\b{roman}\b', digit, text, flags=re.IGNORECASE)
    
    return text


def extract_unit_number(text: str) -> str:
    """
    Extract and normalize unit/plot number.
    
    Args:
        text: Input text containing unit/plot number
        
    Returns:
        Normalized unit number (padded with zeros if numeric)
    """
    if pd.isna(text) or not text:
        return ""
    
    text = str(text).strip()
    
    # Extract numeric part
    numeric_match = re.search(r'(\d+)', text)
    if numeric_match:
        number = numeric_match.group(1)
        # Pad with zeros to 4 digits
        return number.zfill(4)
    
    return text


def normalize_area(area_value: Any) -> float:
    """
    Normalize area to square meters.
    
    Args:
        area_value: Area value (could be string or numeric)
        
    Returns:
        Area in square meters as float
    """
    if pd.isna(area_value):
        return 0.0
    
    try:
        area = float(area_value)
    except (ValueError, TypeError):
        return 0.0
    
    # If area is very small, might be in sqft (convert to sqm)
    if area < 1000:  # Likely sqft
        area = area * 0.092903  # Convert sqft to sqm
    
    return round(area, 2)


def generate_composite_key(row: pd.Series, property_type: str = "apartment") -> str:
    """
    Generate composite key for matching.
    
    Args:
        row: DataFrame row with property data
        property_type: "apartment" or "villa"
        
    Returns:
        Composite key string
    """
    project_clean = row.get('project_clean', '')
    building_clean = row.get('building_clean', '')
    unit_no = row.get('unit_no', '')
    plot_no = row.get('plot_no', '')
    
    if property_type == "apartment":
        # Apartments: project + building + unit
        return f"{project_clean}_{building_clean}_{unit_no}".replace(' ', '_')
    else:
        # Villas: project + plot
        return f"{project_clean}_{plot_no}".replace(' ', '_')


def preprocess_owners(owners_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess owner records for matching.
    
    Args:
        owners_df: Raw owner DataFrame
        
    Returns:
        Cleaned owner DataFrame with unified column names
    """
    logger.info(f"Preprocessing {len(owners_df)} owner records")
    
    # Create a copy to avoid modifying original
    df = owners_df.copy()
    
    # Standardize column names (adjust based on actual column names)
    column_mapping = {
        'Project': 'project',
        'BuildingNameEn': 'building',
        'UnitNumber': 'unit_number',
        ' Size ': 'area',
        'NameEn': 'owner_name'
    }
    
    # Apply column mapping if provided
    if column_mapping:
        df = df.rename(columns=column_mapping)
    
    # Normalize string fields
    string_columns = ['project', 'building', 'unit_number', 'owner_name']
    for col in string_columns:
        if col in df.columns:
            df[f'{col}_clean'] = df[col].apply(normalize_string).apply(replace_synonyms)
    
    # Extract and normalize unit numbers
    if 'unit_number' in df.columns:
        df['unit_no'] = df['unit_number'].apply(extract_unit_number)
    
    # Normalize area
    if 'area' in df.columns:
        df['area_sqm'] = df['area'].apply(normalize_area)
    
    # Generate composite keys
    df['composite_key'] = df.apply(
        lambda row: generate_composite_key(row, "apartment"), axis=1
    )
    
    logger.info(f"Preprocessing complete. Generated {len(df)} cleaned owner records")
    return df


def preprocess_transactions(transactions_df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess transaction records for matching.
    
    Args:
        transactions_df: Raw transaction DataFrame
        
    Returns:
        Cleaned transaction DataFrame with unified column names
    """
    logger.info(f"Preprocessing {len(transactions_df)} transaction records")
    
    # Create a copy to avoid modifying original
    df = transactions_df.copy()
    
    # Standardize column names (adjust based on actual column names)
    column_mapping = {
        'project_name_en': 'project',
        'building_name_en': 'building',
        'unit_number': 'unit_number',
        'procedure_area': 'area',
        'buyer_name': 'buyer_name'
    }
    
    # Apply column mapping if provided
    if column_mapping:
        df = df.rename(columns=column_mapping)
    
    # Normalize string fields
    string_columns = ['project', 'building', 'unit_number', 'buyer_name']
    for col in string_columns:
        if col in df.columns:
            df[f'{col}_clean'] = df[col].apply(normalize_string).apply(replace_synonyms)
    
    # Extract and normalize unit numbers
    # No unit_number column in transactions, so set to empty string
    df['unit_no'] = ""
    
    # Normalize area
    if 'area' in df.columns:
        df['area_sqm'] = df['area'].apply(normalize_area)
    
    # Generate composite keys
    df['composite_key'] = df.apply(
        lambda row: generate_composite_key(row, "apartment"), axis=1
    )
    
    logger.info(f"Preprocessing complete. Generated {len(df)} cleaned transaction records")
    return df


def get_data_info(df: pd.DataFrame, name: str) -> Dict[str, Any]:
    """
    Get basic information about a dataset for QA reporting.
    
    Args:
        df: DataFrame to analyze
        name: Dataset name for logging
        
    Returns:
        Dictionary with dataset statistics
    """
    info = {
        'name': name,
        'total_records': len(df),
        'columns': list(df.columns),
        'missing_values': df.isnull().sum().to_dict(),
        'area_stats': {}
    }
    
    if 'area_sqm' in df.columns:
        info['area_stats'] = {
            'min': df['area_sqm'].min(),
            'max': df['area_sqm'].max(),
            'mean': df['area_sqm'].mean(),
            'median': df['area_sqm'].median()
        }
    
    return info 
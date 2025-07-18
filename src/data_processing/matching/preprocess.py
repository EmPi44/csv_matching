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
from datetime import datetime


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


def normalize_date(date_value: Any) -> str:
    """
    Normalize date fields from DD-MM-YYYY format to YYYY-MM-DD format.
    
    Args:
        date_value: Date value (could be string, datetime, or other format)
        
    Returns:
        Date in YYYY-MM-DD format as string, or empty string if invalid
    """
    if pd.isna(date_value):
        return ""
    
    try:
        # If it's already a datetime object, format it
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        
        # Convert to string
        date_str = str(date_value).strip()
        
        # Handle empty strings
        if not date_str:
            return ""
        
        # Try to parse DD-MM-YYYY format
        if re.match(r'\d{2}-\d{2}-\d{4}', date_str):
            # Parse DD-MM-YYYY format
            parsed_date = datetime.strptime(date_str, '%d-%m-%Y')
            return parsed_date.strftime('%Y-%m-%d')
        
        # Try to parse YYYY-MM-DD format (already correct)
        elif re.match(r'\d{4}-\d{2}-\d{2}', date_str):
            return date_str
        
        # Try to parse MM/DD/YYYY format
        elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
            parsed_date = datetime.strptime(date_str, '%m/%d/%Y')
            return parsed_date.strftime('%Y-%m-%d')
        
        # Try to parse DD/MM/YYYY format
        elif re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
            parsed_date = datetime.strptime(date_str, '%d/%m/%Y')
            return parsed_date.strftime('%Y-%m-%d')
        
        # If none of the above patterns match, try pandas to_datetime
        else:
            parsed_date = pd.to_datetime(date_str, errors='coerce')
            if pd.notna(parsed_date):
                return parsed_date.strftime('%Y-%m-%d')
            else:
                logger.warning(f"Could not parse date: {date_str}")
                return ""
                
    except Exception as e:
        logger.warning(f"Error parsing date '{date_value}': {e}")
        return ""


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


def preprocess_owners(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocess Dubai Hills owners file (raw CSV format).
    Maps columns and generates owner_id if missing.
    """
    # Standardize column names
    df = df.rename(columns={
        'BuildingNameEn': 'building_clean',
        'UnitNumber': 'unit_no',
        ' Size ': 'area_sqm',
        'NameEn': 'owner_name',
        'ProcedurePartyTypeNameEn': 'party_type',
    })
    # Only keep buyers
    df = df[df['party_type'].str.lower() == 'buyer'].copy()
    # Generate owner_id if missing
    if 'owner_id' not in df.columns:
        df['owner_id'] = (
            df['building_clean'].astype(str).str.lower().str.strip() + '_' +
            df['unit_no'].astype(str).str.strip() + '_' +
            df['owner_name'].astype(str).str.lower().str.strip()
        )
    # Clean area
    df['area_sqm'] = pd.to_numeric(df['area_sqm'], errors='coerce')
    # Clean unit_no
    df['unit_no'] = df['unit_no'].astype(str).str.strip()
    # Clean building
    df['building_clean'] = df['building_clean'].astype(str).str.lower().str.strip()
    # Drop duplicates
    df = df.drop_duplicates(subset=['owner_id'])
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
        'transaction_id': 'txn_id',
        'building_name_en': 'building_clean',
        'procedure_area': 'area_sqm',
        'project_name_en': 'project'
    }
    
    # Apply column mapping if provided
    if column_mapping:
        df = df.rename(columns=column_mapping)
    
    # Clean area
    df['area_sqm'] = pd.to_numeric(df['area_sqm'], errors='coerce')
    
    # Clean building name
    df['building_clean'] = df['building_clean'].astype(str).str.lower().str.strip()
    
    # Generate unit_no if missing (use transaction_id as fallback)
    if 'unit_no' not in df.columns:
        df['unit_no'] = df['txn_id'].astype(str).str.strip()
    
    # Clean date fields if they exist
    date_columns = ['instance_date', 'created_at', 'updated_at', 'transaction_date']
    for col in date_columns:
        if col in df.columns:
            logger.info(f"Normalizing date column: {col}")
            df[col] = df[col].apply(normalize_date)
    
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
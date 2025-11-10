"""
Data Cleaning & Standardization Module
Handles: column mapping, string cleaning, money parsing, range splitting
"""

import pandas as pd
import re
from typing import Dict, Optional, List, Union
from pathlib import Path
import json
from loguru import logger


# Load column mapping configuration
def load_column_mapping(config_path: str = "config/column_mapping.json") -> Dict[str, List[str]]:
    """Load column mapping from JSON config file."""
    config_path = Path(config_path)
    if config_path.exists():
        with open(config_path, 'r') as f:
            return json.load(f)
    else:
        logger.warning(f"Column mapping config not found at {config_path}, using defaults")
        return {
            "name": ["Name", "Investor Name", "Fund", "Company Name", "Firm"],
            "location": ["Location", "HQ", "City", "Headquarters", "Base"],
            "country": ["Country", "Nation"],
            "description": ["Description", "About", "Bio", "Overview"],
            "preferred_round": ["Preferred Round", "Round Type", "Stage", "Investment Stage"],
            "deal_size": ["Deal Size Range", "Check Size", "Ticket Size", "Investment Size", "Deal Size"],
            "no_of_rounds": ["No of Rounds", "Number of Rounds", "Rounds", "Total Rounds"],
            "portfolio_value": ["Portfolio Value", "Total Portfolio", "AUM", "Assets Under Management"],
            "notable_companies": ["Notable Companies", "Portfolio Companies", "Investments", "Companies"]
        }


def map_columns(df: pd.DataFrame, column_mapping: Dict[str, List[str]]) -> pd.DataFrame:
    """
    Map various column name variations to standard names.
    
    Args:
        df: Input DataFrame
        column_mapping: Dictionary mapping standard names to possible variations
        
    Returns:
        DataFrame with standardized column names
    """
    df = df.copy()
    mapping_dict = {}
    
    # Build mapping from variations to standard names
    for standard_name, variations in column_mapping.items():
        for variation in variations:
            # Case-insensitive matching
            for col in df.columns:
                if col.lower().strip() == variation.lower().strip():
                    mapping_dict[col] = standard_name
                    break
    
    # Rename columns
    if mapping_dict:
        df = df.rename(columns=mapping_dict)
        logger.info(f"Mapped columns: {mapping_dict}")
    
    return df


def clean_string(value: str) -> Optional[str]:
    """Clean and normalize string values."""
    if pd.isna(value) or value is None:
        return None
    
    if not isinstance(value, str):
        value = str(value)
    
    # Strip whitespace
    value = value.strip()
    
    # Remove extra whitespace
    value = re.sub(r'\s+', ' ', value)
    
    # Return None for empty strings
    if not value or value.lower() in ['nan', 'none', 'null', 'n/a', 'na']:
        return None
    
    return value


def parse_money(value: Union[str, float, int]) -> Optional[float]:
    """
    Parse money strings like "$500k", "$11.0b", "$1.5M" to float values.
    
    Args:
        value: Money string or number
        
    Returns:
        Float value in base units (dollars), or None if unparseable
    """
    if pd.isna(value) or value is None:
        return None
    
    # If already a number, return it
    if isinstance(value, (int, float)):
        return float(value)
    
    if not isinstance(value, str):
        value = str(value)
    
    # Remove currency symbols (both $ and €) and whitespace
    value = re.sub(r'[\$€,\s]', '', value.strip())
    
    if not value or value.lower() in ['nan', 'none', 'null', 'n/a', 'na', '-', '']:
        return None
    
    # Extract number and multiplier
    match = re.match(r'([\d.]+)\s*([kmbKMB]?)$', value, re.IGNORECASE)
    if match:
        number = float(match.group(1))
        multiplier = match.group(2).upper()
        
        multipliers = {
            'K': 1_000,
            'M': 1_000_000,
            'B': 1_000_000_000
        }
        
        return number * multipliers.get(multiplier, 1)
    
    # Try to parse as plain number
    try:
        return float(value)
    except ValueError:
        logger.warning(f"Could not parse money value: {value}")
        return None


def parse_range(value: Union[str, float]) -> tuple:
    """
    Parse range strings like "$500k - $11.0b" or "1M-5M" into (min, max).
    
    Args:
        value: Range string or single value
        
    Returns:
        Tuple of (min_value, max_value) as floats, or (None, None)
    """
    if pd.isna(value) or value is None:
        return (None, None)
    
    if isinstance(value, (int, float)):
        return (float(value), float(value))
    
    if not isinstance(value, str):
        value = str(value)
    
    value = value.strip()
    
    # Look for range separators
    separators = [' - ', '-', ' to ', '–', '—']
    for sep in separators:
        if sep in value:
            parts = value.split(sep, 1)
            if len(parts) == 2:
                min_val = parse_money(parts[0].strip())
                max_val = parse_money(parts[1].strip())
                return (min_val, max_val)
    
    # Single value
    parsed = parse_money(value)
    if parsed is not None:
        return (parsed, parsed)
    
    return (None, None)


def extract_country_from_sheet(sheet_name: str) -> Optional[str]:
    """
    Extract country name from sheet name if it contains a country.
    This is a simple heuristic - can be enhanced with a country list.
    """
    # Common country names (can be expanded)
    countries = [
        "Austria", "Belgium", "Bulgaria", "Croatia", "Cyprus", "Czech Republic",
        "Denmark", "Estonia", "Finland", "France", "Germany", "Greece", "Hungary",
        "Ireland", "Italy", "Latvia", "Lithuania", "Luxembourg", "Malta", "Netherlands",
        "Poland", "Portugal", "Romania", "Slovakia", "Slovenia", "Spain", "Sweden",
        "United Kingdom", "UK", "USA", "United States", "Canada", "Australia", "Japan",
        "China", "India", "Brazil", "Mexico", "Switzerland", "Norway", "Israel"
    ]
    
    sheet_lower = sheet_name.lower()
    for country in countries:
        if country.lower() in sheet_lower:
            return country
    
    return None


def clean_dataframe(df: pd.DataFrame, 
                   sheet_name: str = None,
                   source_file: str = None,
                   column_mapping: Dict = None) -> pd.DataFrame:
    """
    Main cleaning function that standardizes a DataFrame.
    
    Args:
        df: Input DataFrame
        sheet_name: Name of the sheet (for country extraction)
        source_file: Source filename (for metadata)
        column_mapping: Column mapping dictionary
        
    Returns:
        Cleaned and standardized DataFrame
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for cleaning")
        return df
    
    df = df.copy()
    original_rows = len(df)
    
    # Load column mapping if not provided
    if column_mapping is None:
        column_mapping = load_column_mapping()
    
    # Map columns to standard names
    df = map_columns(df, column_mapping)
    
    # Clean string columns
    string_columns = ['name', 'location', 'description', 'preferred_round', 'notable_companies']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].apply(clean_string)
    
    # Parse deal size range
    if 'deal_size' in df.columns:
        ranges = df['deal_size'].apply(parse_range)
        df['deal_size_min'] = ranges.apply(lambda x: x[0])
        df['deal_size_max'] = ranges.apply(lambda x: x[1])
        df = df.drop(columns=['deal_size'], errors='ignore')
    elif 'deal_size_min' not in df.columns:
        df['deal_size_min'] = None
    elif 'deal_size_max' not in df.columns:
        df['deal_size_max'] = None
    
    # Parse portfolio value
    if 'portfolio_value' in df.columns:
        df['portfolio_value'] = df['portfolio_value'].apply(parse_money)
    
    # Parse no_of_rounds as integer
    if 'no_of_rounds' in df.columns:
        df['no_of_rounds'] = pd.to_numeric(df['no_of_rounds'], errors='coerce').astype('Int64')
    
    # Extract country from sheet name if country column is missing
    if 'country' not in df.columns or df['country'].isna().all():
        if sheet_name:
            country = extract_country_from_sheet(sheet_name)
            if country:
                df['country'] = country
                logger.info(f"Extracted country '{country}' from sheet name: {sheet_name}")
    
    # Clean country column
    if 'country' in df.columns:
        df['country'] = df['country'].apply(clean_string)
    
    # Add source metadata
    df['source_file'] = source_file if source_file else None
    df['source_sheet'] = sheet_name if sheet_name else None
    df['ingested_at'] = pd.Timestamp.now()
    
    # Ensure all standard columns exist first
    standard_columns = [
        'name', 'description', 'preferred_round', 'location', 'country',
        'deal_size_min', 'deal_size_max', 'no_of_rounds', 'portfolio_value',
        'notable_companies', 'source_file', 'source_sheet', 'ingested_at'
    ]
    
    # Remove rows where name is missing (low quality)
    # Check if 'name' column exists before trying to drop rows
    if 'name' in df.columns:
        df = df.dropna(subset=['name'])
    else:
        logger.warning(f"No 'name' column found after cleaning. Sheet may be empty or have no mappable columns.")
        # Return empty dataframe with standard columns
        df = pd.DataFrame(columns=standard_columns)
        return df
    
    for col in standard_columns:
        if col not in df.columns:
            df[col] = None
    
    # Reorder columns
    df = df[standard_columns]
    
    final_rows = len(df)
    logger.info(f"Cleaned DataFrame: {original_rows} -> {final_rows} rows")
    
    return df


"""
Universal File Ingest Module
Supports: .xlsx, .csv, .tsv, .json, .parquet
Returns: Dictionary of DataFrames (sheet_name -> DataFrame)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, Union
from loguru import logger


def load_file(filepath: Union[str, Path]) -> Dict[str, pd.DataFrame]:
    """
    Load a file and return a dictionary of DataFrames.
    For Excel files, returns all sheets. For others, returns {"data": DataFrame}.
    
    Args:
        filepath: Path to the file to load
        
    Returns:
        Dictionary mapping sheet names to DataFrames
        
    Raises:
        ValueError: If file format is not supported
        FileNotFoundError: If file doesn't exist
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    ext = filepath.suffix.lower()
    logger.info(f"Loading file: {filepath.name} (format: {ext})")
    
    try:
        if ext == '.xlsx':
            # Load all sheets from Excel
            sheets = pd.read_excel(filepath, sheet_name=None, engine='openpyxl')
            logger.info(f"Loaded {len(sheets)} sheets from Excel file")
            return sheets
            
        elif ext == '.csv':
            df = pd.read_csv(filepath, encoding='utf-8', low_memory=False)
            logger.info(f"Loaded CSV with {len(df)} rows")
            return {"data": df}
            
        elif ext == '.tsv':
            df = pd.read_csv(filepath, sep='\t', encoding='utf-8', low_memory=False)
            logger.info(f"Loaded TSV with {len(df)} rows")
            return {"data": df}
            
        elif ext == '.json':
            df = pd.read_json(filepath, encoding='utf-8')
            logger.info(f"Loaded JSON with {len(df)} rows")
            return {"data": df}
            
        elif ext == '.parquet':
            df = pd.read_parquet(filepath)
            logger.info(f"Loaded Parquet with {len(df)} rows")
            return {"data": df}
            
        else:
            raise ValueError(f"Unsupported file format: {ext}. Supported: .xlsx, .csv, .tsv, .json, .parquet")
            
    except Exception as e:
        logger.error(f"Error loading file {filepath}: {str(e)}")
        raise


def filter_sheets(sheets: Dict[str, pd.DataFrame], 
                  exclude_keywords: list = None) -> Dict[str, pd.DataFrame]:
    """
    Filter out sheets with unwanted names (e.g., "Overview", "Search & scrape").
    
    Args:
        sheets: Dictionary of sheet names to DataFrames
        exclude_keywords: List of keywords to exclude (case-insensitive)
        
    Returns:
        Filtered dictionary of sheets
    """
    if exclude_keywords is None:
        exclude_keywords = ["overview", "search", "scrape", "instructions", "readme", "metadata"]
    
    filtered = {}
    for sheet_name, df in sheets.items():
        sheet_lower = sheet_name.lower()
        if not any(keyword in sheet_lower for keyword in exclude_keywords):
            filtered[sheet_name] = df
        else:
            logger.info(f"Skipping sheet: {sheet_name} (matches exclude keywords)")
    
    logger.info(f"Filtered to {len(filtered)} sheets from {len(sheets)} total")
    return filtered


def get_file_info(filepath: Union[str, Path]) -> dict:
    """
    Get metadata about a file.
    
    Args:
        filepath: Path to the file
        
    Returns:
        Dictionary with file metadata
    """
    filepath = Path(filepath)
    return {
        "filename": filepath.name,
        "extension": filepath.suffix.lower(),
        "size_bytes": filepath.stat().st_size if filepath.exists() else 0,
        "modified_at": pd.Timestamp.fromtimestamp(filepath.stat().st_mtime) if filepath.exists() else None
    }


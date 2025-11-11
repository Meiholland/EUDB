"""
Dynamic Schema Management
Automatically detects and adds new columns to the database
"""

import sqlite3
import pandas as pd
from typing import Set, Dict, List
from loguru import logger


def detect_column_type(series: pd.Series) -> str:
    """
    Detect the appropriate SQLite type for a pandas Series.
    
    Args:
        series: Pandas Series to analyze
        
    Returns:
        SQLite type string (TEXT, REAL, INTEGER)
    """
    # Remove nulls for type detection
    non_null = series.dropna()
    
    if len(non_null) == 0:
        return 'TEXT'  # Default to TEXT if all nulls
    
    # Check if it's numeric
    if pd.api.types.is_numeric_dtype(series):
        # Check if it's integer-like
        if pd.api.types.is_integer_dtype(series):
            return 'INTEGER'
        else:
            return 'REAL'
    
    # Check if it's datetime
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TEXT'  # Store as ISO format string
    
    # Check if values look like numbers
    try:
        numeric_count = 0
        for val in non_null.head(100):  # Sample first 100 non-null values
            try:
                float(str(val).replace(',', '').replace('$', '').replace('€', ''))
                numeric_count += 1
            except:
                pass
        
        # If >80% look numeric, might be REAL (but keep as TEXT for flexibility)
        if numeric_count / min(len(non_null), 100) > 0.8:
            return 'TEXT'  # Keep as TEXT to preserve formatting
    except:
        pass
    
    # Default to TEXT
    return 'TEXT'


def normalize_column_name(col_name: str) -> str:
    """
    Normalize column names for database storage.
    - Convert to lowercase
    - Replace spaces/special chars with underscores
    - Remove leading/trailing whitespace
    
    Args:
        col_name: Original column name
        
    Returns:
        Normalized column name safe for SQL
    """
    import re
    # Convert to lowercase
    normalized = str(col_name).lower().strip()
    # Replace spaces and special chars with underscores
    normalized = re.sub(r'[^\w]+', '_', normalized)
    # Remove multiple underscores
    normalized = re.sub(r'_+', '_', normalized)
    # Remove leading/trailing underscores
    normalized = normalized.strip('_')
    # Ensure it's not empty and doesn't start with a number
    if not normalized or normalized[0].isdigit():
        normalized = 'col_' + normalized
    return normalized


def get_all_columns_from_dataframe(df: pd.DataFrame) -> Dict[str, str]:
    """
    Analyze a DataFrame and return column names with their detected types.
    
    Args:
        df: DataFrame to analyze
        
    Returns:
        Dictionary mapping normalized column names to SQLite types
    """
    columns = {}
    for col in df.columns:
        normalized = normalize_column_name(col)
        col_type = detect_column_type(df[col])
        columns[normalized] = col_type
        if normalized != col.lower().strip():
            logger.debug(f"Normalized column '{col}' -> '{normalized}'")
    return columns


def ensure_columns_exist(conn: sqlite3.Connection, columns: Dict[str, str]) -> Set[str]:
    """
    Ensure all columns exist in the database, adding them if they don't.
    
    Args:
        conn: Database connection
        columns: Dictionary mapping column names to SQLite types
        
    Returns:
        Set of column names that were added
    """
    cursor = conn.cursor()
    
    # Get existing columns
    cursor.execute("PRAGMA table_info(investors)")
    existing_columns = {row[1].lower() for row in cursor.fetchall()}
    
    added_columns = set()
    
    # Add missing columns
    for col_name, col_type in columns.items():
        if col_name not in existing_columns:
            try:
                # SQLite doesn't support adding NOT NULL columns to existing tables easily
                # So we add them as nullable
                cursor.execute(f"ALTER TABLE investors ADD COLUMN {col_name} {col_type}")
                logger.info(f"Added new column '{col_name}' ({col_type}) to database")
                added_columns.add(col_name)
            except sqlite3.OperationalError as e:
                logger.warning(f"Could not add column '{col_name}': {e}")
    
    conn.commit()
    return added_columns


def scan_and_update_schema(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """
    Scan a DataFrame, detect all columns, and update database schema if needed.
    
    Args:
        conn: Database connection
        df: DataFrame to scan
    """
    if df.empty:
        return
    
    # Get all columns with their types
    columns = get_all_columns_from_dataframe(df)
    
    # Ensure they exist in database
    added = ensure_columns_exist(conn, columns)
    
    if added:
        logger.info(f"Schema updated: Added {len(added)} new column(s): {', '.join(added)}")


def get_all_database_columns(conn: sqlite3.Connection) -> List[str]:
    """
    Get all column names from the investors table.
    
    Args:
        conn: Database connection
        
    Returns:
        List of column names
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(investors)")
    return [row[1] for row in cursor.fetchall()]


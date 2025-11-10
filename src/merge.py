"""
Smart Merging & Deduplication Module
Handles fuzzy matching and intelligent merging of investor data
"""

import pandas as pd
from typing import Optional, List, Tuple
from pathlib import Path
from loguru import logger
from rapidfuzz import fuzz, process

from database import init_database, insert_dataframe, load_all_investors
from clean import clean_dataframe


def fuzzy_match_name_location(name1: str, location1: str, 
                              name2: str, location2: str,
                              threshold: int = 85) -> bool:
    """
    Check if two investor records are likely the same using fuzzy matching.
    
    Args:
        name1, location1: First record
        name2, location2: Second record
        threshold: Similarity threshold (0-100)
        
    Returns:
        True if records are likely duplicates
    """
    if pd.isna(name1) or pd.isna(name2):
        return False
    
    name1 = str(name1).lower().strip()
    name2 = str(name2).lower().strip()
    
    # Exact match on name
    if name1 == name2:
        # If locations match (or both missing), it's a duplicate
        loc1 = str(location1).lower().strip() if not pd.isna(location1) else ""
        loc2 = str(location2).lower().strip() if not pd.isna(location2) else ""
        if loc1 == loc2 or (not loc1 and not loc2):
            return True
    
    # Fuzzy match on name
    name_similarity = fuzz.ratio(name1, name2)
    if name_similarity >= threshold:
        # Check location similarity
        if pd.isna(location1) or pd.isna(location2):
            return True  # If one location is missing, trust name match
        loc1 = str(location1).lower().strip()
        loc2 = str(location2).lower().strip()
        loc_similarity = fuzz.ratio(loc1, loc2)
        if loc_similarity >= 70:  # Location similarity threshold
            return True
    
    return False


def find_duplicates(new_df: pd.DataFrame, 
                   existing_df: pd.DataFrame,
                   threshold: int = 85) -> pd.DataFrame:
    """
    Find duplicate records between new and existing data.
    
    Args:
        new_df: New DataFrame to check
        existing_df: Existing DataFrame to compare against
        threshold: Fuzzy matching threshold
        
    Returns:
        DataFrame with duplicate matches (new_index, existing_index, similarity)
    """
    if new_df.empty or existing_df.empty:
        return pd.DataFrame(columns=['new_index', 'existing_index', 'similarity'])
    
    duplicates = []
    
    for new_idx, new_row in new_df.iterrows():
        new_name = str(new_row.get('name', '')).lower().strip()
        new_location = str(new_row.get('location', '')).lower().strip() if not pd.isna(new_row.get('location')) else ""
        
        if not new_name:
            continue
        
        # Find best match in existing data
        best_match = None
        best_similarity = 0
        
        for existing_idx, existing_row in existing_df.iterrows():
            existing_name = str(existing_row.get('name', '')).lower().strip()
            existing_location = str(existing_row.get('location', '')).lower().strip() if not pd.isna(existing_row.get('location')) else ""
            
            if fuzzy_match_name_location(new_name, new_location, 
                                        existing_name, existing_location, 
                                        threshold):
                name_sim = fuzz.ratio(new_name, existing_name)
                if name_sim > best_similarity:
                    best_similarity = name_sim
                    best_match = existing_idx
        
        if best_match is not None:
            duplicates.append({
                'new_index': new_idx,
                'existing_index': best_match,
                'similarity': best_similarity
            })
    
    return pd.DataFrame(duplicates)


def merge_strategy_keep_latest(new_row: pd.Series, existing_row: pd.Series) -> pd.Series:
    """Merge strategy: Keep the latest record (by ingested_at)."""
    if pd.to_datetime(new_row.get('ingested_at', pd.Timestamp.min)) > \
       pd.to_datetime(existing_row.get('ingested_at', pd.Timestamp.min)):
        return new_row
    return existing_row


def merge_strategy_keep_richest(new_row: pd.Series, existing_row: pd.Series) -> pd.Series:
    """Merge strategy: Keep the record with higher portfolio_value."""
    new_value = new_row.get('portfolio_value', 0) or 0
    existing_value = existing_row.get('portfolio_value', 0) or 0
    
    if new_value > existing_value:
        return new_row
    return existing_row


def merge_strategy_merge_fields(new_row: pd.Series, existing_row: pd.Series) -> pd.Series:
    """Merge strategy: Combine fields, preferring non-null values."""
    merged = existing_row.copy()
    
    for col in new_row.index:
        new_val = new_row[col]
        existing_val = existing_row.get(col)
        
        # Prefer new value if existing is null
        if pd.isna(existing_val) and not pd.isna(new_val):
            merged[col] = new_val
        # Prefer new value if it's more recent
        elif col == 'ingested_at':
            if pd.to_datetime(new_val) > pd.to_datetime(existing_val):
                merged[col] = new_val
        # For numeric fields, take the maximum
        elif col in ['portfolio_value', 'deal_size_max', 'no_of_rounds']:
            if not pd.isna(new_val) and (pd.isna(existing_val) or new_val > existing_val):
                merged[col] = new_val
    
    return merged


def deduplicate_and_merge(new_df: pd.DataFrame,
                         existing_df: pd.DataFrame,
                         strategy: str = "keep_latest",
                         threshold: int = 85) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deduplicate and merge new data with existing data.
    
    Args:
        new_df: New DataFrame to merge
        existing_df: Existing DataFrame
        strategy: Merge strategy ("keep_latest", "keep_richest", "merge_fields")
        threshold: Fuzzy matching threshold
        
    Returns:
        Tuple of (unique_new_records, updated_existing_records)
    """
    if existing_df.empty:
        logger.info("No existing data, all new records are unique")
        return new_df, existing_df
    
    if new_df.empty:
        logger.info("No new data to merge")
        return new_df, existing_df
    
    # Find duplicates
    duplicates = find_duplicates(new_df, existing_df, threshold)
    
    logger.info(f"Found {len(duplicates)} potential duplicates")
    
    # Get strategy function
    strategy_funcs = {
        "keep_latest": merge_strategy_keep_latest,
        "keep_richest": merge_strategy_keep_richest,
        "merge_fields": merge_strategy_merge_fields
    }
    
    merge_func = strategy_funcs.get(strategy, merge_strategy_keep_latest)
    
    # Process duplicates
    new_indices_to_remove = set()
    existing_indices_to_update = {}
    
    for _, dup in duplicates.iterrows():
        new_idx = dup['new_index']
        existing_idx = dup['existing_index']
        
        new_row = new_df.loc[new_idx]
        existing_row = existing_df.loc[existing_idx]
        
        # Apply merge strategy
        merged_row = merge_func(new_row, existing_row)
        
        # If merged row is the existing one, remove from new
        if merged_row.equals(existing_row):
            new_indices_to_remove.add(new_idx)
        else:
            # Update existing record
            existing_indices_to_update[existing_idx] = merged_row
            new_indices_to_remove.add(new_idx)
    
    # Remove duplicates from new_df
    unique_new = new_df.drop(index=new_indices_to_remove).copy()
    
    # Update existing records
    updated_existing = existing_df.copy()
    for idx, merged_row in existing_indices_to_update.items():
        updated_existing.loc[idx] = merged_row
    
    logger.info(f"After deduplication: {len(unique_new)} new unique records, "
                f"{len(existing_indices_to_update)} existing records updated")
    
    return unique_new, updated_existing


def ingest_and_merge(filepath: str,
                    db_path: str = "data/investors.db",
                    merge_strategy: str = "keep_latest",
                    fuzzy_threshold: int = 85) -> dict:
    """
    Complete pipeline: ingest file, clean, merge with existing data, save to DB.
    
    Args:
        filepath: Path to file to ingest
        db_path: Path to SQLite database
        merge_strategy: Merge strategy to use
        fuzzy_threshold: Fuzzy matching threshold
        
    Returns:
        Dictionary with processing results
    """
    from ingest import load_file, filter_sheets, get_file_info
    
    filepath = Path(filepath)
    results = {
        "file": str(filepath),
        "sheets_processed": 0,
        "rows_added": 0,
        "rows_updated": 0,
        "rows_skipped": 0,
        "errors": []
    }
    
    try:
        # Load file
        sheets = load_file(filepath)
        sheets = filter_sheets(sheets)
        
        # Initialize database
        conn = init_database(db_path)
        
        # Load existing data
        existing_df = load_all_investors(db_path)
        
        # Process each sheet
        for sheet_name, df in sheets.items():
            try:
                # Clean the data
                cleaned_df = clean_dataframe(
                    df,
                    sheet_name=sheet_name,
                    source_file=filepath.name
                )
                
                if cleaned_df.empty:
                    logger.warning(f"Sheet {sheet_name} produced no valid rows after cleaning")
                    continue
                
                # Deduplicate and merge
                unique_new, updated_existing = deduplicate_and_merge(
                    cleaned_df,
                    existing_df,
                    strategy=merge_strategy,
                    threshold=fuzzy_threshold
                )
                
                # Insert new unique records
                if not unique_new.empty:
                    rows_added = insert_dataframe(conn, unique_new)
                    results["rows_added"] += rows_added
                
                # Update existing records (delete old, insert new)
                if not updated_existing.empty and len(updated_existing) > len(existing_df):
                    # This is simplified - in production, you'd want to update in place
                    # For now, we'll just insert and let the unique constraint handle it
                    pass
                
                results["sheets_processed"] += 1
                
            except Exception as e:
                error_msg = f"Error processing sheet {sheet_name}: {str(e)}"
                logger.error(error_msg)
                results["errors"].append(error_msg)
        
        conn.close()
        
        logger.info(f"Processing complete: {results}")
        return results
        
    except Exception as e:
        error_msg = f"Error processing file {filepath}: {str(e)}"
        logger.error(error_msg)
        results["errors"].append(error_msg)
        return results


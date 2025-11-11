"""
SQLite Database Operations Module
Creates and manages the investors database (Supabase-ready schema)
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List, Dict
from loguru import logger
from datetime import datetime
try:
    from .dynamic_schema import scan_and_update_schema, normalize_column_name
except ImportError:
    # Handle case where running as script
    from dynamic_schema import scan_and_update_schema, normalize_column_name


# SQLite schema (Supabase-ready)
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS investors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    preferred_round TEXT,
    location TEXT,
    country TEXT,
    deal_size_min REAL,
    deal_size_max REAL,
    no_of_rounds INTEGER,
    portfolio_value REAL,
    notable_companies TEXT,
    exit_total_value REAL,
    website TEXT,
    email TEXT,
    phone TEXT,
    founded TEXT,
    employees TEXT,
    source_file TEXT,
    source_sheet TEXT,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, location)
);
"""

# Create indexes for better query performance
INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_name ON investors(name);",
    "CREATE INDEX IF NOT EXISTS idx_location ON investors(location);",
    "CREATE INDEX IF NOT EXISTS idx_country ON investors(country);",
    "CREATE INDEX IF NOT EXISTS idx_source_file ON investors(source_file);"
]


def migrate_database(conn: sqlite3.Connection) -> None:
    """
    Legacy migration function - now handled by dynamic_schema.
    This function is kept for backward compatibility but does nothing.
    The dynamic schema system (scan_and_update_schema) handles adding columns
    automatically when data is inserted.
    """
    # Migration is now handled dynamically by scan_and_update_schema()
    # when data is inserted. No hardcoded columns to add.
    pass


def get_column_usage_stats(conn: sqlite3.Connection) -> Dict[str, Dict]:
    """
    Get statistics about column usage (how many non-null values each column has).
    
    Args:
        conn: Database connection
        
    Returns:
        Dictionary mapping column names to usage statistics
    """
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(investors)")
    columns = [row[1] for row in cursor.fetchall()]
    
    stats = {}
    total_rows = cursor.execute("SELECT COUNT(*) FROM investors").fetchone()[0]
    
    for col in columns:
        if col == 'id':
            continue
        # Count non-null values
        cursor.execute(f"SELECT COUNT(*) FROM investors WHERE {col} IS NOT NULL AND {col} != ''")
        non_null_count = cursor.fetchone()[0]
        usage_percent = (non_null_count / total_rows * 100) if total_rows > 0 else 0
        
        stats[col] = {
            'non_null_count': non_null_count,
            'total_rows': total_rows,
            'usage_percent': usage_percent,
            'is_used': non_null_count > 0
        }
    
    return stats


def get_unused_columns(conn: sqlite3.Connection, min_usage_percent: float = 0.0) -> List[str]:
    """
    Get list of columns that are unused (all NULL or below usage threshold).
    
    Args:
        conn: Database connection
        min_usage_percent: Minimum usage percentage to consider a column "used" (0-100)
        
    Returns:
        List of unused column names
    """
    stats = get_column_usage_stats(conn)
    unused = []
    
    for col, stat in stats.items():
        if stat['usage_percent'] <= min_usage_percent:
            unused.append(col)
    
    return unused


def remove_unused_columns(conn: sqlite3.Connection, columns_to_remove: List[str], 
                         preserve_essential: bool = True) -> int:
    """
    Remove unused columns from the database.
    Note: SQLite doesn't support DROP COLUMN directly, so we need to recreate the table.
    
    Args:
        conn: Database connection
        columns_to_remove: List of column names to remove
        preserve_essential: If True, never remove essential columns (name, location, etc.)
        
    Returns:
        Number of columns removed
    """
    if not columns_to_remove:
        return 0
    
    essential_columns = {'id', 'name', 'location', 'source_file', 'source_sheet', 'ingested_at'}
    
    # Filter out essential columns if preserve_essential is True
    if preserve_essential:
        columns_to_remove = [col for col in columns_to_remove if col not in essential_columns]
    
    if not columns_to_remove:
        logger.info("No removable columns (all are essential)")
        return 0
    
    cursor = conn.cursor()
    
    # Get all current columns
    cursor.execute("PRAGMA table_info(investors)")
    all_columns = [row[1] for row in cursor.fetchall()]
    
    # Get columns to keep
    columns_to_keep = [col for col in all_columns if col not in columns_to_remove]
    
    if len(columns_to_keep) == len(all_columns):
        logger.info("No columns to remove")
        return 0
    
    logger.info(f"Removing {len(columns_to_remove)} unused columns: {columns_to_remove}")
    
    # SQLite doesn't support DROP COLUMN, so we need to recreate the table
    # This is a complex operation - we'll create a new table, copy data, then replace
    
    # Step 1: Create new table with only columns we want to keep
    columns_def = []
    for col in columns_to_keep:
        if col == 'id':
            columns_def.append(f"{col} INTEGER PRIMARY KEY AUTOINCREMENT")
        elif col == 'name':
            columns_def.append(f"{col} TEXT NOT NULL")
        elif col in ['deal_size_min', 'deal_size_max', 'portfolio_value', 'exit_total_value']:
            columns_def.append(f"{col} REAL")
        elif col == 'no_of_rounds':
            columns_def.append(f"{col} INTEGER")
        elif col == 'ingested_at':
            columns_def.append(f"{col} DATETIME DEFAULT CURRENT_TIMESTAMP")
        else:
            columns_def.append(f"{col} TEXT")
    
    # Create new table
    new_table_sql = f"""
    CREATE TABLE investors_new (
        {', '.join(columns_def)}
    )
    """
    
    cursor.execute(new_table_sql)
    
    # Step 2: Copy data (only columns that exist in both)
    columns_str = ', '.join(columns_to_keep)
    cursor.execute(f"INSERT INTO investors_new ({columns_str}) SELECT {columns_str} FROM investors")
    
    # Step 3: Drop old table and rename new one
    cursor.execute("DROP TABLE investors")
    cursor.execute("ALTER TABLE investors_new RENAME TO investors")
    
    # Step 4: Recreate indexes
    for index_sql in INDEXES_SQL:
        try:
            cursor.execute(index_sql)
        except:
            pass
    
    conn.commit()
    logger.info(f"Successfully removed {len(columns_to_remove)} columns")
    
    return len(columns_to_remove)


def init_database(db_path: str = "data/investors.db") -> sqlite3.Connection:
    """
    Initialize the database with schema and indexes.
    Also migrates existing databases to add new columns.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Database connection
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Check if table exists
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='investors'")
    table_exists = cursor.fetchone() is not None
    
    if not table_exists:
        # Create schema for new database
        conn.executescript(SCHEMA_SQL)
        logger.info(f"Created new database at {db_path}")
    else:
        # Database exists - dynamic schema will handle adding columns as needed
        # No need to run migration (columns are added when data is inserted)
        logger.debug(f"Database exists at {db_path}, using dynamic schema")
    
    # Create indexes
    for index_sql in INDEXES_SQL:
        conn.execute(index_sql)
    
    conn.commit()
    logger.info(f"Database initialized at {db_path}")
    
    return conn


def insert_dataframe(conn: sqlite3.Connection, df: pd.DataFrame, 
                    replace: bool = False) -> int:
    """
    Insert DataFrame into the investors table.
    Automatically detects and adds new columns to the schema.
    
    Args:
        conn: Database connection
        df: DataFrame to insert
        replace: If True, replace existing rows (by name+location)
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        logger.warning("Empty DataFrame provided for insertion")
        return 0
    
    # First, scan the DataFrame and update schema if needed
    scan_and_update_schema(conn, df)
    
    # Prepare DataFrame for insertion
    insert_df = df.copy()
    
    # Normalize all column names to match database schema
    column_mapping = {}
    for col in insert_df.columns:
        normalized = normalize_column_name(col)
        if normalized != col:
            column_mapping[col] = normalized
    
    if column_mapping:
        insert_df = insert_df.rename(columns=column_mapping)
        logger.debug(f"Normalized columns: {column_mapping}")
    
    # Convert to SQL-compatible types
    for col in ['deal_size_min', 'deal_size_max', 'portfolio_value']:
        if col in insert_df.columns:
            insert_df[col] = pd.to_numeric(insert_df[col], errors='coerce')
    
    if 'no_of_rounds' in insert_df.columns:
        insert_df['no_of_rounds'] = pd.to_numeric(insert_df['no_of_rounds'], errors='coerce').astype('Int64')
    
    # Convert datetime columns to strings for SQLite compatibility
    for col in insert_df.columns:
        if insert_df[col].dtype == 'datetime64[ns]' or pd.api.types.is_datetime64_any_dtype(insert_df[col]):
            insert_df[col] = insert_df[col].dt.strftime('%Y-%m-%d %H:%M:%S')
        elif 'ingested_at' in col and insert_df[col].dtype == 'object':
            # Try to convert if it's a datetime string
            try:
                insert_df[col] = pd.to_datetime(insert_df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
    
    # Use direct SQL insertion to handle UNIQUE constraints and data type conversion
    # This is more reliable than pandas' to_sql with custom methods
    try:
        cursor = conn.cursor()
        placeholders = ', '.join(['?' for _ in insert_df.columns])
        columns = ', '.join(insert_df.columns)
        
        if replace:
            sql = f"INSERT OR REPLACE INTO investors ({columns}) VALUES ({placeholders})"
        else:
            sql = f"INSERT OR IGNORE INTO investors ({columns}) VALUES ({placeholders})"
        
        inserted = 0
        for _, row in insert_df.iterrows():
            try:
                values = []
                for col in insert_df.columns:
                    val = row[col]
                    # Convert to SQLite-compatible types
                    if pd.isna(val):
                        values.append(None)
                    elif isinstance(val, pd.Timestamp):
                        values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    elif hasattr(pd, 'NaT') and (val is pd.NaT or isinstance(val, type(pd.NaT))):
                        values.append(None)
                    elif isinstance(val, (int, float)) and pd.isna(val):
                        values.append(None)
                    else:
                        values.append(val)
                
                cursor.execute(sql, values)
                if cursor.rowcount > 0:
                    inserted += 1
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                logger.debug(f"Skipping row due to error: {e}")
                continue
        
        conn.commit()
        logger.info(f"Inserted {inserted} rows into database")
        return inserted
        
    except Exception as e:
        logger.error(f"Error inserting data: {e}")
        conn.rollback()
        # Fallback to row-by-row insertion
        inserted = 0
        cursor = conn.cursor() if hasattr(conn, 'cursor') else conn
        for _, row in insert_df.iterrows():
            try:
                row_dict = row.to_dict()
                # Convert values to SQLite-compatible types
                values = []
                for key, val in row_dict.items():
                    if pd.isna(val):
                        values.append(None)
                    elif isinstance(val, pd.Timestamp):
                        values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                    elif hasattr(pd, 'NaT') and (val is pd.NaT or isinstance(val, type(pd.NaT))):
                        values.append(None)
                    else:
                        values.append(val)
                
                placeholders = ', '.join(['?' for _ in row_dict.keys()])
                columns = ', '.join(row_dict.keys())
                sql = f"INSERT OR IGNORE INTO investors ({columns}) VALUES ({placeholders})"
                cursor.execute(sql, values)
                inserted += 1
            except (sqlite3.IntegrityError, sqlite3.OperationalError) as e:
                logger.debug(f"Skipping row due to error: {e}")
                continue
        if hasattr(conn, 'commit'):
            conn.commit()
        logger.info(f"Inserted {inserted} rows (fallback method)")
        return inserted


def load_all_investors(db_path: str = "data/investors.db") -> pd.DataFrame:
    """
    Load all investors from the database.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        DataFrame with all investors
    """
    db_path = Path(db_path)
    if not db_path.exists():
        logger.warning(f"Database not found at {db_path}, returning empty DataFrame")
        return pd.DataFrame()
    
    conn = sqlite3.connect(str(db_path))
    df = pd.read_sql_query("SELECT * FROM investors ORDER BY name", conn)
    conn.close()
    
    logger.info(f"Loaded {len(df)} investors from database")
    return df


def search_investors(db_path: str = "data/investors.db",
                    name: Optional[str] = None,
                    country: Optional[str] = None,
                    location: Optional[str] = None,
                    min_deal_size: Optional[float] = None,
                    max_deal_size: Optional[float] = None,
                    search_text: Optional[str] = None) -> pd.DataFrame:
    """
    Search investors with various filters.
    
    Args:
        db_path: Path to SQLite database file
        name: Filter by name (partial match)
        country: Filter by country
        location: Filter by location
        min_deal_size: Minimum deal size
        max_deal_size: Maximum deal size
        search_text: Full-text search across all columns
        
    Returns:
        Filtered DataFrame
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return pd.DataFrame()
    
    conn = sqlite3.connect(str(db_path))
    
    query = "SELECT * FROM investors WHERE 1=1"
    params = []
    
    if name:
        query += " AND name LIKE ?"
        params.append(f"%{name}%")
    
    if country:
        query += " AND country = ?"
        params.append(country)
    
    if location:
        query += " AND location LIKE ?"
        params.append(f"%{location}%")
    
    if min_deal_size is not None:
        query += " AND deal_size_max >= ?"
        params.append(min_deal_size)
    
    if max_deal_size is not None:
        query += " AND deal_size_max <= ?"
        params.append(max_deal_size)
    
    if search_text:
        query += " AND (name LIKE ? OR description LIKE ? OR location LIKE ? OR notable_companies LIKE ?)"
        search_param = f"%{search_text}%"
        params.extend([search_param] * 4)
    
    query += " ORDER BY name"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    
    logger.info(f"Search returned {len(df)} results")
    return df


def get_statistics(db_path: str = "data/investors.db") -> dict:
    """
    Get database statistics.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Dictionary with statistics
    """
    db_path = Path(db_path)
    if not db_path.exists():
        return {
            "total_investors": 0,
            "countries": [],
            "sources": []
        }
    
    conn = sqlite3.connect(str(db_path))
    
    stats = {}
    
    # Total count
    cursor = conn.execute("SELECT COUNT(*) FROM investors")
    stats["total_investors"] = cursor.fetchone()[0]
    
    # Countries
    cursor = conn.execute("SELECT DISTINCT country FROM investors WHERE country IS NOT NULL ORDER BY country")
    stats["countries"] = [row[0] for row in cursor.fetchall()]
    
    # Source files
    cursor = conn.execute("SELECT DISTINCT source_file FROM investors WHERE source_file IS NOT NULL ORDER BY source_file")
    stats["sources"] = [row[0] for row in cursor.fetchall()]
    
    # Column usage stats
    stats["column_usage"] = get_column_usage_stats(conn)
    
    conn.close()
    
    return stats


def export_schema(db_path: str = "data/investors.db", 
                 output_path: str = "schema.sql") -> str:
    """
    Export database schema as SQL file (for Supabase import).
    
    Args:
        db_path: Path to SQLite database file
        output_path: Path to output SQL file
        
    Returns:
        Path to exported schema file
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, 'w') as f:
        f.write("-- Investor Database Schema (Supabase-ready)\n")
        f.write("-- Generated automatically\n\n")
        f.write(SCHEMA_SQL)
        f.write("\n")
        for index_sql in INDEXES_SQL:
            f.write(index_sql + "\n")
    
    logger.info(f"Schema exported to {output_path}")
    return str(output_path)

"""
SQLite Database Operations Module
Creates and manages the investors database (Supabase-ready schema)
"""

import sqlite3
import pandas as pd
from pathlib import Path
from typing import Optional, List
from loguru import logger
from datetime import datetime


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


def init_database(db_path: str = "data/investors.db") -> sqlite3.Connection:
    """
    Initialize the database with schema and indexes.
    
    Args:
        db_path: Path to SQLite database file
        
    Returns:
        Database connection
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(str(db_path))
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Create schema
    conn.executescript(SCHEMA_SQL)
    
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
    
    # Prepare DataFrame for insertion
    insert_df = df.copy()
    
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
                    elif isinstance(val, (pd._libs.tslibs.nattype.NaTType, type(pd.NaT))):
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


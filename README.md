# Investor Data Hub

**Universal Investor Data Cleaner & Search App (Multi-Source, Export-Ready)**

A flexible, production-ready Python application that ingests investor data from multiple tabular sources, cleans and standardizes it, merges into a unified database, and provides an interactive UI for searching, filtering, and exporting.

## 🎯 Features

| Feature | Description |
|---------|-------------|
| **Multi-Format Upload** | Supports `.xlsx`, `.csv`, `.tsv`, `.json`, `.parquet` |
| **Auto Sheet Detection** | Automatically skips unwanted sheets like "Overview", "Search & scrape", etc. |
| **Smart Column Mapping** | Maps variations like `Deal Size Range`, `Deal Size (min - max)` → `deal_size_min`, `deal_size_max` |
| **Intelligent Deduplication** | Fuzzy matching by `Name + Location` with configurable threshold |
| **Source Tracking** | Adds `source_file`, `source_sheet`, `ingested_at` metadata |
| **Live Merge** | New data automatically merges with existing database |
| **Export Options** | CSV, JSON, SQL dump (Supabase-ready) |
| **Search & Filter** | Full-text search, multi-filter, regex support |

## 🚀 Quick Start

### 1. Setup

```bash
# Install dependencies (use pip3 on macOS)
pip3 install -r requirements.txt

# Or use python3 -m pip
python3 -m pip install -r requirements.txt
```

### 2. Run the Application

```bash
# Use python3 -m streamlit to avoid PATH issues
python3 -m streamlit run src/app.py

# Or if streamlit is in your PATH:
streamlit run src/app.py
```

The app will open in your browser at `http://localhost:8501`

### 3. Use the App

1. **Upload Files**: Go to the "Upload & Process" tab and drop your `.xlsx`, `.csv`, or other supported files
2. **Clean & Merge**: Click "Clean & Merge" to process files and add to database
3. **Search & Filter**: Use the "Search & Filter" tab to find specific investors
4. **Export**: Download clean data as CSV, JSON, or SQL dump

## 📁 Project Structure

```
investor-app/
├── data/
│   ├── raw/                  # ← Drop ANY files here (.xlsx, .csv, .json, etc.)
│   ├── cleaned_investors.csv  # Auto-generated export
│   └── investors.db           # ← Auto-updated SQLite database
├── src/
│   ├── ingest.py             # Load + detect file type
│   ├── clean.py              # Clean, standardize, dedupe
│   ├── database.py           # SQLite operations
│   ├── merge.py              # Combine new + existing
│   └── app.py                # Streamlit UI
├── config/
│   └── column_mapping.json   # Standardize messy column names
├── logs/
│   └── app.log               # Application logs
├── requirements.txt
└── README.md
```

## 🔧 Configuration

### Column Mapping

Edit `config/column_mapping.json` to customize how column names are mapped to standard names:

```json
{
  "name": ["Name", "Investor Name", "Fund"],
  "location": ["Location", "HQ", "City"],
  "deal_size": ["Deal Size Range", "Check Size", "Ticket Size"]
}
```

## 📊 Database Schema

The SQLite database uses a Supabase-ready schema:

```sql
CREATE TABLE investors (
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
```

## 🔄 Export to Supabase

### Method 1: SQL Dump

```bash
# Export SQLite database
sqlite3 data/investors.db ".dump" > dump.sql

# Then import into Supabase via SQL editor
```

### Method 2: CSV Import

1. Export data as CSV from the app
2. Use Supabase's CSV import feature
3. Map columns as needed

### Method 3: JSON Import

1. Export data as JSON from the app
2. Use Supabase's JSON import or API

## 🎨 Usage Examples

### Upload Multiple Files

1. Go to "Upload & Process" tab
2. Select multiple files (`.xlsx`, `.csv`, etc.)
3. Choose merge strategy:
   - **keep_latest**: Keep the most recently ingested record
   - **keep_richest**: Keep record with highest portfolio value
   - **merge_fields**: Combine fields, preferring non-null values
4. Set fuzzy match threshold (70-100, higher = stricter)
5. Click "Clean & Merge"

### Search & Filter

- **Full-text Search**: Searches across all fields
- **Country Filter**: Multi-select dropdown
- **Advanced Filters**: Location, deal size range
- **Export Results**: Download filtered results as CSV or JSON

### View All Data

- Browse all investors in the database
- Export entire dataset
- View statistics in sidebar

## 🛠️ Advanced Features

### Merge Strategies

- **keep_latest**: Prefers records with more recent `ingested_at` timestamp
- **keep_richest**: Prefers records with higher `portfolio_value`
- **merge_fields**: Intelligently combines fields from both records

### Fuzzy Matching

Uses `rapidfuzz` library for intelligent duplicate detection:
- Name similarity matching (configurable threshold)
- Location similarity matching
- Handles typos, abbreviations, and variations

### Data Cleaning

- **String Normalization**: Strips whitespace, normalizes case
- **Money Parsing**: Converts "$500k", "$11.0b" → numeric values
- **Range Splitting**: Splits "500k - 11b" → `deal_size_min`, `deal_size_max`
- **Country Extraction**: Extracts country from sheet names when available

## 📝 Logging

Logs are written to `logs/app.log` using `loguru`:
- File processing status
- Cleaning operations
- Database operations
- Errors and warnings

## 🔮 Future Enhancements

- [ ] **Review Duplicates Tab**: Manual merge interface for duplicate records
- [ ] **Google Sheets Integration**: Direct import from Google Sheets API
- [ ] **Supabase Direct Sync**: One-click sync to Supabase
- [ ] **Schema Export**: Auto-generate `schema.sql` for Supabase
- [ ] **Data Validation**: Enhanced validation rules and error reporting
- [ ] **Batch Processing**: Process entire directories of files
- [ ] **API Endpoints**: REST API for programmatic access

## 🐛 Troubleshooting

### Common Issues

**Issue**: "Module not found" errors
- **Solution**: Make sure you've installed all dependencies: `pip install -r requirements.txt`

**Issue**: Excel files not loading
- **Solution**: Ensure `openpyxl` is installed: `pip install openpyxl`

**Issue**: Database locked errors
- **Solution**: Close any other connections to the database file

**Issue**: Column mapping not working
- **Solution**: Check `config/column_mapping.json` format and column names in your files

## 📄 License

This project is open source and available for use.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

---

**Built with ❤️ for flexible investor data management**


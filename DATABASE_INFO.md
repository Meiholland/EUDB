# Database Information & Code Updates

## What Happens After You Upload a File?

When you upload and process a file:

1. **File is saved** to `data/raw/` directory
2. **Data is cleaned** and standardized
3. **Data is merged** with existing database (deduplication happens)
4. **Database is updated** - new rows are added, duplicates are handled based on merge strategy
5. **Data persists** - The SQLite database (`data/investors.db`) stores all your data

## Database Location

- **Local**: `data/investors.db` (on your computer)
- **Streamlit Cloud**: `data/investors.db` (on Streamlit's servers - **persists between sessions**)

## What Happens When You Update the Code?

### ✅ **Good News: Your Data is Safe!**

The database **automatically migrates** when you update the code. Here's what happens:

1. **New Columns Added Automatically**: If we add new columns (like `exit_total_value`), the database migration function adds them to existing tables
2. **Existing Data Preserved**: All your existing investor records remain intact
3. **No Data Loss**: The migration only adds new columns, it doesn't delete anything

### Example Scenario:

**Before Update:**
- Database has: `name`, `location`, `description`, etc.
- You have 1,763 investors

**After Code Update (adding `exit_total_value` column):**
- Database now has: `name`, `location`, `description`, **`exit_total_value`**, etc.
- You still have 1,763 investors (all data preserved)
- New column is empty for existing records (will be filled when you re-upload files)

## When Do You Need to Reset the Database?

You **rarely need to reset** the database. Only reset if:

1. **Schema changes break compatibility** (very rare - we handle migrations)
2. **You want to start fresh** (clear all data)
3. **Data corruption** (extremely rare)

### How to Reset (if needed):

**Local:**
```bash
rm data/investors.db
# Then restart the app - it will create a new empty database
```

**Streamlit Cloud:**
- Go to your app settings
- Delete the `data/investors.db` file (if accessible)
- Or contact Streamlit support
- Or: Re-upload files will merge/update, you don't need to reset

## Database Migration System

The app includes automatic database migration:

- **`migrate_database()`** function checks for new columns
- **Adds missing columns** automatically when the app starts
- **Preserves all existing data**
- **No manual intervention needed**

## Best Practices

1. **Backup your database** before major updates (export as CSV/JSON)
2. **Re-upload files** after adding new column mappings to populate new fields
3. **Use merge strategies** to handle duplicates intelligently
4. **Export regularly** - Download CSV/JSON backups from the app

## Current Supported Columns

### Standard Columns (always present):
- `name`, `description`, `preferred_round`, `location`, `country`
- `deal_size_min`, `deal_size_max`, `no_of_rounds`, `portfolio_value`
- `notable_companies`, `exit_total_value`, `website`, `email`, `phone`
- `founded`, `employees`
- `source_file`, `source_sheet`, `ingested_at`

### Adding New Columns:

1. Add to `config/column_mapping.json`
2. Add to `standard_columns` list in `src/clean.py`
3. Add to database schema in `src/database.py`
4. Add to `migrate_database()` function
5. Push code - migration happens automatically!

## Summary

✅ **Data persists** between code updates  
✅ **Automatic migrations** add new columns  
✅ **No data loss** when updating code  
✅ **Re-upload files** to populate new columns  
❌ **No need to reset** database for code updates  


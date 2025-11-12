"""
Streamlit UI for Investor Data Hub
Main application interface for uploading, cleaning, searching, and exporting data
"""

import streamlit as st
import pandas as pd
import json
import sys
import io
from pathlib import Path
from datetime import datetime

# Import our modules

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from ingest import load_file, filter_sheets, get_file_info
from clean import clean_dataframe, load_column_mapping
from database import (
    init_database, load_all_investors, search_investors,
    get_statistics, export_schema, get_column_usage_stats,
    get_unused_columns, remove_unused_columns, update_investor_from_dataframe
)
from merge import ingest_and_merge

# Page configuration
st.set_page_config(
    page_title="Investor Data Hub",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'original_filtered_df' not in st.session_state:
    st.session_state.original_filtered_df = pd.DataFrame()
if 'last_saved_df' not in st.session_state:
    st.session_state.last_saved_df = pd.DataFrame()


def load_data():
    """Load data from database and update session state."""
    try:
        db_path = "data/investors.db"
        st.session_state.df = load_all_investors(db_path)
        st.session_state.data_loaded = True
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        st.session_state.df = pd.DataFrame()
        st.session_state.data_loaded = False


# Sidebar
with st.sidebar:
    st.title("📊 Investor Data Hub")
    st.markdown("---")
    
    # Statistics
    st.subheader("📈 Statistics")
    try:
        stats = get_statistics("data/investors.db")
        st.metric("Total Investors", stats.get("total_investors", 0))
        st.metric("Countries", len(stats.get("countries", [])))
        st.metric("Source Files", len(stats.get("sources", [])))
    except:
        st.info("No data yet. Upload files to get started!")
    
    st.markdown("---")
    
    # Refresh button
    if st.button("🔄 Refresh Data", width='stretch'):
        load_data()
        st.success("Data refreshed!")
    
    # Export schema
    if st.button("📋 Export Schema", width='stretch'):
        try:
            schema_path = export_schema("data/investors.db", "schema.sql")
            with open(schema_path, 'r') as f:
                st.download_button(
                    "Download Schema SQL",
                    f.read(),
                    "schema.sql",
                    "text/plain"
                )
        except Exception as e:
            st.error(f"Error exporting schema: {str(e)}")


# Main content
st.title("📊 Investor Data Hub")
st.markdown("**Universal Investor Data Cleaner & Search App**")

# Tabs
tab1, tab2, tab3, tab4 = st.tabs(["📤 Upload & Process", "🔍 Search & Filter", "📊 View All Data", "⚙️ Settings"])

# Tab 1: Upload & Process
with tab1:
    st.header("Upload & Process Files")
    st.markdown("Upload investor data files (.xlsx, .csv, .tsv, .json, .parquet)")
    
    uploaded_files = st.file_uploader(
        "Choose files to upload",
        type=['xlsx', 'csv', 'tsv', 'json', 'parquet'],
        accept_multiple_files=True,
        help="You can upload multiple files at once"
    )
    
    if uploaded_files:
        st.info(f"📁 {len(uploaded_files)} file(s) selected")
        
        # Merge strategy selection
        col1, col2 = st.columns(2)
        with col1:
            merge_strategy = st.selectbox(
                "Merge Strategy",
                ["keep_latest", "keep_richest", "merge_fields"],
                help="How to handle duplicate records"
            )
        with col2:
            fuzzy_threshold = st.slider(
                "Fuzzy Match Threshold",
                min_value=70,
                max_value=100,
                value=85,
                help="Similarity threshold for duplicate detection (higher = stricter)"
            )
        
        if st.button("🚀 Clean & Merge", type="primary", width='stretch'):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            results_summary = {
                "files_processed": 0,
                "total_rows_added": 0,
                "total_errors": 0
            }
            
            # Ensure data/raw directory exists
            Path("data/raw").mkdir(parents=True, exist_ok=True)
            
            for idx, uploaded_file in enumerate(uploaded_files):
                try:
                    status_text.text(f"Processing {uploaded_file.name}...")
                    
                    # Save uploaded file
                    save_path = f"data/raw/{uploaded_file.name}"
                    with open(save_path, "wb") as f:
                        f.write(uploaded_file.getbuffer())
                    
                    # Process file
                    result = ingest_and_merge(
                        save_path,
                        db_path="data/investors.db",
                        merge_strategy=merge_strategy,
                        fuzzy_threshold=fuzzy_threshold
                    )
                    
                    results_summary["files_processed"] += 1
                    results_summary["total_rows_added"] += result.get("rows_added", 0)
                    results_summary["total_errors"] += len(result.get("errors", []))
                    
                    progress_bar.progress((idx + 1) / len(uploaded_files))
                    
                except Exception as e:
                    st.error(f"Error processing {uploaded_file.name}: {str(e)}")
                    results_summary["total_errors"] += 1
            
            status_text.empty()
            progress_bar.empty()
            
            # Show results
            st.success(f"✅ Processing complete!")
            st.json(results_summary)
            
            # Reload data
            load_data()
            
            if results_summary["total_errors"] > 0:
                st.warning(f"⚠️ {results_summary['total_errors']} error(s) occurred. Check logs for details.")

# Tab 2: Search & Filter
with tab2:
    st.header("Search & Filter Investors")
    
    # Load data if not loaded
    if not st.session_state.data_loaded:
        load_data()
    
    if st.session_state.df.empty:
        st.info("📭 No data available. Upload files in the 'Upload & Process' tab.")
    else:
        # Search and filter controls
        col1, col2 = st.columns(2)
        
        with col1:
            search_text = st.text_input(
                "🔍 Full-text Search",
                placeholder="Search across all fields...",
                help="Searches in name, description, location, and notable companies"
            )
        
        with col2:
            country_filter = st.multiselect(
                "🌍 Filter by Country",
                options=sorted(st.session_state.df['country'].dropna().unique()) if 'country' in st.session_state.df.columns else [],
                help="Select one or more countries"
            )
        
        # Additional filters
        with st.expander("🔧 Advanced Filters"):
            col1, col2, col3 = st.columns(3)
            
            with col1:
                location_filter = st.text_input("📍 Location (contains)")
            
            with col2:
                min_deal_size = st.number_input(
                    "💰 Min Deal Size ($)",
                    min_value=0.0,
                    value=0.0,
                    step=1000.0,
                    format="%.0f"
                )
            
            with col3:
                # Use a large finite number instead of infinity
                max_default = 1_000_000_000_000.0  # 1 trillion as default max
                if not st.session_state.df.empty and st.session_state.df['deal_size_max'].notna().any():
                    max_default = float(st.session_state.df['deal_size_max'].max()) * 1.1  # 10% above max
                
                max_deal_size = st.number_input(
                    "💰 Max Deal Size ($)",
                    min_value=0.0,
                    value=max_default,
                    step=1000.0,
                    format="%.0f",
                    help="Leave at default or set to filter by maximum deal size"
                )
        
        # Apply filters
        filtered_df = st.session_state.df.copy()
        
        if search_text:
            mask = filtered_df.apply(
                lambda row: search_text.lower() in str(row).lower(),
                axis=1
            )
            filtered_df = filtered_df[mask]
        
        if country_filter:
            filtered_df = filtered_df[filtered_df['country'].isin(country_filter)]
        
        if location_filter:
            filtered_df = filtered_df[
                filtered_df['location'].str.contains(location_filter, case=False, na=False)
            ]
        
        if min_deal_size > 0:
            filtered_df = filtered_df[
                (filtered_df['deal_size_max'] >= min_deal_size) |
                (filtered_df['deal_size_max'].isna())
            ]
        
        # Only apply max filter if it's been changed from a very large default
        # Use 1 trillion as threshold to detect if user actually set a limit
        if max_deal_size < 1_000_000_000_000.0:
            filtered_df = filtered_df[
                (filtered_df['deal_size_max'] <= max_deal_size) |
                (filtered_df['deal_size_max'].isna())
            ]
        
        # Display results
        st.metric("Results", len(filtered_df))
        
        if not filtered_df.empty:
            # Store original for comparison (only if it's a new filter)
            filter_key = f"{search_text}_{country_filter}_{location_filter}_{min_deal_size}_{max_deal_size}"
            if 'current_filter_key' not in st.session_state or st.session_state.current_filter_key != filter_key:
                st.session_state.original_filtered_df = filtered_df.copy()
                st.session_state.last_saved_df = filtered_df.copy()
                st.session_state.current_filter_key = filter_key
            
            # Prepare editable dataframe (keep original numeric values for editing)
            editable_df = filtered_df.copy()
            
            # Use data_editor for inline editing
            st.info("💡 **Tip**: Double-click any cell to edit. Changes are saved automatically when you press Enter or click outside the cell.")
            
            edited_df = st.data_editor(
                editable_df,
                key=f"investor_editor_{filter_key}",
                width='stretch',
                height=600,
                num_rows="fixed",
                disabled=["id"],  # Don't allow editing the ID
                hide_index=True
            )
            
            # Check if data was modified by comparing with last saved version
            # Reset index for proper comparison
            edited_comparison = edited_df.reset_index(drop=True)
            saved_comparison = st.session_state.last_saved_df.reset_index(drop=True)
            
            # Compare dataframes (handles NaN properly)
            if not edited_comparison.equals(saved_comparison):
                # Find changed rows by comparing each row
                if 'id' in edited_comparison.columns and 'id' in saved_comparison.columns:
                    changed_rows = []
                    for idx in range(min(len(edited_comparison), len(saved_comparison))):
                        row_edited = edited_comparison.iloc[idx]
                        row_original = saved_comparison.iloc[idx]
                        
                        # Compare values, handling NaN properly
                        values_changed = False
                        for col in edited_comparison.columns:
                            if col == 'id':
                                continue
                            val_edited = row_edited[col]
                            val_original = row_original[col]
                            
                            # Handle NaN comparison
                            if pd.isna(val_edited) and pd.isna(val_original):
                                continue
                            elif pd.isna(val_edited) or pd.isna(val_original):
                                values_changed = True
                                break
                            elif val_edited != val_original:
                                values_changed = True
                                break
                        
                        if values_changed:
                            changed_rows.append(edited_comparison.iloc[[idx]])
                    
                    if changed_rows:
                        # Save changes to database
                        try:
                            conn = init_database("data/investors.db")
                            changes_df = pd.concat(changed_rows, ignore_index=True)
                            updated_count = update_investor_from_dataframe(conn, changes_df)
                            conn.close()
                            
                            if updated_count > 0:
                                st.success(f"✅ Saved {updated_count} change(s) to database!")
                                # Update the saved dataframe
                                st.session_state.last_saved_df = edited_df.copy()
                                # Reload data to reflect changes
                                load_data()
                                st.rerun()
                        except Exception as e:
                            st.error(f"Error saving changes: {str(e)}")
            
            # Export options (use edited dataframe)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv = edited_df.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    csv,
                    f"investors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    "text/csv",
                    width='stretch'
                )
            
            with col2:
                json_str = edited_df.to_json(orient='records', indent=2)
                st.download_button(
                    "📥 Download JSON",
                    json_str,
                    f"investors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    "application/json",
                    width='stretch'
                )
            
            with col3:
                # Copy to clipboard button (for Supabase)
                st.code(json_str[:500] + "..." if len(json_str) > 500 else json_str, language="json")
                st.caption("Copy JSON above for Supabase import")
        else:
            st.info("No results match your filters. Try adjusting your search criteria.")

# Tab 3: View All Data
with tab3:
    st.header("View All Data")
    
    if not st.session_state.data_loaded:
        load_data()
    
    if st.session_state.df.empty:
        st.info("📭 No data available. Upload files in the 'Upload & Process' tab.")
    else:
        st.metric("Total Investors", len(st.session_state.df))
        
        # Initialize saved dataframe for "View All Data" tab
        if 'original_all_df' not in st.session_state:
            st.session_state.original_all_df = st.session_state.df.copy()
            st.session_state.last_saved_all_df = st.session_state.df.copy()
        
        # Update if the underlying data changed
        if not st.session_state.df.equals(st.session_state.original_all_df):
            st.session_state.original_all_df = st.session_state.df.copy()
            st.session_state.last_saved_all_df = st.session_state.df.copy()
        
        st.info("💡 **Tip**: Double-click any cell to edit. Changes are saved automatically when you press Enter or click outside the cell.")
        
        # Use data_editor for inline editing
        edited_all_df = st.data_editor(
            st.session_state.df.copy(),
            key="investor_editor_all",
            width='stretch',
            height=600,
            num_rows="fixed",
            disabled=["id"],  # Don't allow editing the ID
            hide_index=True
        )
        
        # Check if data was modified by comparing with last saved version
        # Reset index for proper comparison
        edited_all_comparison = edited_all_df.reset_index(drop=True)
        saved_all_comparison = st.session_state.last_saved_all_df.reset_index(drop=True)
        
        # Compare dataframes (handles NaN properly)
        if not edited_all_comparison.equals(saved_all_comparison):
            # Find changed rows by comparing each row
            if 'id' in edited_all_comparison.columns and 'id' in saved_all_comparison.columns:
                changed_rows = []
                for idx in range(min(len(edited_all_comparison), len(saved_all_comparison))):
                    row_edited = edited_all_comparison.iloc[idx]
                    row_original = saved_all_comparison.iloc[idx]
                    
                    # Compare values, handling NaN properly
                    values_changed = False
                    for col in edited_all_comparison.columns:
                        if col == 'id':
                            continue
                        val_edited = row_edited[col]
                        val_original = row_original[col]
                        
                        # Handle NaN comparison
                        if pd.isna(val_edited) and pd.isna(val_original):
                            continue
                        elif pd.isna(val_edited) or pd.isna(val_original):
                            values_changed = True
                            break
                        elif val_edited != val_original:
                            values_changed = True
                            break
                    
                    if values_changed:
                        changed_rows.append(edited_all_comparison.iloc[[idx]])
                
                if changed_rows:
                    # Save changes to database
                    try:
                        conn = init_database("data/investors.db")
                        changes_df = pd.concat(changed_rows, ignore_index=True)
                        updated_count = update_investor_from_dataframe(conn, changes_df)
                        conn.close()
                        
                        if updated_count > 0:
                            st.success(f"✅ Saved {updated_count} change(s) to database!")
                            # Update the saved dataframe
                            st.session_state.last_saved_all_df = edited_all_df.copy()
                            # Reload data to reflect changes
                            load_data()
                            st.rerun()
                    except Exception as e:
                        st.error(f"Error saving changes: {str(e)}")
        
        # Export all (use edited dataframe)
        col1, col2 = st.columns(2)
        with col1:
            csv_all = edited_all_df.to_csv(index=False)
            st.download_button(
                "📥 Download All (CSV)",
                csv_all,
                "cleaned_investors.csv",
                "text/csv",
                width='stretch'
            )
        with col2:
            json_all = edited_all_df.to_json(orient='records', indent=2)
            st.download_button(
                "📥 Download All (JSON)",
                json_all,
                "cleaned_investors.json",
                "application/json",
                width='stretch'
            )

# Tab 4: Settings
with tab4:
    st.header("⚙️ Settings & Configuration")
    
    st.subheader("Column Mapping")
    st.markdown("Configure how column names are mapped to standard names.")
    
    try:
        column_mapping = load_column_mapping()
        st.json(column_mapping)
        
        # Allow editing
        edited_mapping = st.text_area(
            "Edit Column Mapping (JSON)",
            value=json.dumps(column_mapping, indent=2),
            height=400
        )
        
        if st.button("💾 Save Column Mapping"):
            try:
                Path("config").mkdir(parents=True, exist_ok=True)
                with open("config/column_mapping.json", "w") as f:
                    f.write(edited_mapping)
                st.success("Column mapping saved!")
            except Exception as e:
                st.error(f"Error saving: {str(e)}")
    except Exception as e:
        st.error(f"Error loading column mapping: {str(e)}")
    
    st.markdown("---")
    
    st.subheader("🔍 Research & Update Investors")
    st.markdown("Automatically research and update investor descriptions and websites.")
    
    research_col1, research_col2 = st.columns(2)
    with research_col1:
        research_country = st.selectbox(
            "Select Country",
            options=sorted(st.session_state.df['country'].dropna().unique()) if not st.session_state.df.empty and 'country' in st.session_state.df.columns else [],
            help="Select a country to research investors from"
        )
    with research_col2:
        research_limit = st.number_input(
            "Limit (optional)",
            min_value=1,
            max_value=100,
            value=10,
            help="Maximum number of investors to research"
        )
    
    if st.button("🔍 Research Investors", type="primary"):
        st.info("⚠️ **Note**: Web research requires API access. This feature needs to be run with web search capabilities enabled.")
        st.warning("For now, please use the research script manually or integrate with a web search API.")
        
        # Placeholder for future implementation
        # This would call the research function when web search is available
        st.code(f"""
# To research investors, run:
# python3 research_investors.py --country {research_country} --limit {research_limit}

# Or integrate with web search API in the research.py module
        """)
    
    st.markdown("---")
    
    st.subheader("Column Usage & Cleanup")
    st.markdown("View which columns are being used and remove unused columns.")
    
    try:
        conn = init_database("data/investors.db")
        column_stats = get_column_usage_stats(conn)
        unused_cols = get_unused_columns(conn, min_usage_percent=0.0)
        
        if column_stats:
            # Show column usage table
            usage_data = []
            for col, stats in sorted(column_stats.items()):
                usage_data.append({
                    "Column": col,
                    "Used": f"{stats['non_null_count']:,}",
                    "Total": f"{stats['total_rows']:,}",
                    "Usage %": f"{stats['usage_percent']:.1f}%",
                    "Status": "✅ Used" if stats['is_used'] else "❌ Unused"
                })
            
            usage_df = pd.DataFrame(usage_data)
            st.dataframe(usage_df, width='stretch', height=400)
            
            # Show unused columns
            if unused_cols:
                st.warning(f"⚠️ Found {len(unused_cols)} unused column(s): {', '.join(unused_cols)}")
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("🗑️ Remove Unused Columns", type="primary"):
                        try:
                            removed = remove_unused_columns(conn, unused_cols, preserve_essential=True)
                            st.success(f"✅ Removed {removed} unused column(s)!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error removing columns: {str(e)}")
                
                with col2:
                    st.caption("This will permanently remove columns with no data. Essential columns (name, location, etc.) are protected.")
            else:
                st.success("✅ All columns are being used!")
        
        conn.close()
    except Exception as e:
        st.error(f"Error loading column stats: {str(e)}")
    
    st.markdown("---")
    
    st.subheader("Database Info")
    try:
        stats = get_statistics("data/investors.db")
        # Remove column_usage from main stats display (shown above)
        display_stats = {k: v for k, v in stats.items() if k != 'column_usage'}
        st.json(display_stats)
    except Exception as e:
        st.error(f"Error loading statistics: {str(e)}")
    
    st.markdown("---")
    
    st.subheader("Export Database")
    st.markdown("Export the SQLite database for Supabase import.")
    
    if st.button("📤 Export SQL Dump"):
        try:
            import subprocess
            db_path = "data/investors.db"
            dump_path = "investors_dump.sql"
            
            result = subprocess.run(
                ["sqlite3", db_path, ".dump"],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                with open(dump_path, "w") as f:
                    f.write(result.stdout)
                
                with open(dump_path, "r") as f:
                    st.download_button(
                        "📥 Download SQL Dump",
                        f.read(),
                        "investors_dump.sql",
                        "text/plain"
                    )
                st.success("SQL dump created!")
            else:
                st.error(f"Error creating dump: {result.stderr}")
        except Exception as e:
            st.error(f"Error: {str(e)}")
            st.info("Make sure sqlite3 is installed and available in PATH")

# Initialize on first load
if not st.session_state.data_loaded:
    load_data()


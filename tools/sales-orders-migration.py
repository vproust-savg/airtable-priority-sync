"""
Sales Orders Export Script
Exports Output sheet from Excel to multiple tab-delimited .txt files
Split by 10,000 unique orders (keeping all line items together)
"""

import pandas as pd
import os
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_FILE = "/Users/victorproust/Documents/Work/Priority/Dynamic Data/Sales Orders/WORK - Sales Orders MIG.xlsx"
OUTPUT_BASE_DIR = "/Users/victorproust/Documents/Work/Priority/Dynamic Data/Sales Orders/MIG"
ORDERS_PER_FILE = 10000

# =============================================================================
# FUNCTIONS
# =============================================================================

def thorough_clean(value):
    """Apply thorough cleaning: remove line breaks, tabs, non-breaking spaces, and trim."""
    if pd.isna(value) or value == "":
        return ""
    s = str(value)
    s = s.replace('\n', ' ')      # CHAR(10) - Line feed
    s = s.replace('\r', ' ')      # CHAR(13) - Carriage return
    s = s.replace('\xa0', ' ')    # CHAR(160) - Non-breaking space
    s = s.replace('\t', ' ')      # CHAR(9) - Tab
    s = ' '.join(s.split())       # Collapse multiple spaces and trim
    return s

def format_date(value):
    """Format date as MM/DD/YY."""
    if pd.isna(value):
        return ""
    if isinstance(value, pd.Timestamp):
        return value.strftime('%m/%d/%y')
    return str(value)

def format_number(value):
    """Round number to 2 decimals."""
    if pd.isna(value):
        return ""
    try:
        return round(float(value), 2)
    except (ValueError, TypeError):
        return value

# =============================================================================
# MAIN SCRIPT
# =============================================================================

def main():
    # Timestamp for folder and file names
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    
    # Create output folder
    output_folder = os.path.join(OUTPUT_BASE_DIR, f"export_{timestamp}")
    os.makedirs(output_folder, exist_ok=True)
    print(f"Output folder: {output_folder}")
    
    # Read Excel file - Output sheet only
    print(f"Reading: {SOURCE_FILE}")
    df = pd.read_excel(SOURCE_FILE, sheet_name='Output')
    print(f"Total rows: {len(df)}")
    
    # Process data - apply formatting to each column
    print("Processing data...")
    
    df_processed = pd.DataFrame()
    
    # Column A: Order ID Trim (thorough clean)
    df_processed['Order ID Trim'] = df['Order ID Trim'].apply(thorough_clean)
    
    # Column B: Customer PO Number + Order ID (thorough clean - formula already in Excel)
    df_processed['Customer PO Number + Order ID'] = df['Customer PO Number + Order ID'].apply(thorough_clean)
    
    # Column C: Cust ID Trim (thorough clean)
    df_processed['Cust ID Trim'] = df['Cust ID Trim'].apply(thorough_clean)
    
    # Column D: Date Created (format MM/DD/YY)
    df_processed['Date Created'] = df['Date Created'].apply(format_date)
    
    # Column E: SKU Trim2 (thorough clean)
    df_processed['SKU Trim2'] = df['SKU Trim2'].apply(thorough_clean)
    
    # Column F: Part Name (thorough clean)
    df_processed['Part Name'] = df['Part Name'].apply(thorough_clean)
    
    # Column G: Qty (rounded to 2 decimals)
    df_processed['Qty'] = df['Orders line items::Qty_converted'].apply(format_number)
    
    # Column H: Item Price (rounded to 2 decimals)
    df_processed['Item Price'] = df['Orders line items::Item Price Round'].apply(format_number)
    
    # Column I: Date Due (format MM/DD/YY)
    df_processed['Date Due'] = df['Date Due'].apply(format_date)
    
    # Get unique order IDs in order of appearance
    unique_orders = df_processed['Order ID Trim'].unique()
    total_unique_orders = len(unique_orders)
    print(f"Unique orders: {total_unique_orders}")
    
    # Split by 10,000 unique orders
    file_num = 1
    start_order_idx = 0
    
    while start_order_idx < total_unique_orders:
        end_order_idx = min(start_order_idx + ORDERS_PER_FILE, total_unique_orders)
        
        # Get orders for this batch
        batch_orders = unique_orders[start_order_idx:end_order_idx]
        
        # Filter rows for these orders
        batch_df = df_processed[df_processed['Order ID Trim'].isin(batch_orders)]
        
        # Generate filename
        filename = f"sales_orders_{timestamp}_part_{file_num:02d}.txt"
        filepath = os.path.join(output_folder, filename)
        
        # Save as tab-delimited, no header, no index
        batch_df.to_csv(filepath, sep='\t', index=False, header=False)
        
        print(f"Created: {filename} ({len(batch_orders)} orders, {len(batch_df)} rows)")
        
        file_num += 1
        start_order_idx = end_order_idx
    
    print(f"\nDone! Created {file_num - 1} files in: {output_folder}")

if __name__ == "__main__":
    main()

"""
Data Loader — Reads lead files (Excel, CSV, JSON) from the leads folder.
"""
import os
import pandas as pd
import glob

DEFAULT_LEADS_PATH = r'C:\Users\tdeca\OneDrive\Desktop\empresa\Projeto CRM Leads'

def load_leads(folder_path=None):
    """
    Load all lead files from the given folder.
    Supports .xlsx, .csv, and .json formats.
    Returns a combined pandas DataFrame with normalized column names.
    """
    if folder_path is None:
        folder_path = DEFAULT_LEADS_PATH
    
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Leads folder not found: {folder_path}")

    all_dfs = []

    # Find all supported files
    patterns = ['*.xlsx', '*.csv', '*.json']
    files = []
    for pattern in patterns:
        files.extend(glob.glob(os.path.join(folder_path, pattern)))

    # Exclude temp/lock files
    files = [f for f in files if not os.path.basename(f).startswith('.~lock')]

    if not files:
        raise FileNotFoundError(f"No data files found in: {folder_path}")

    for filepath in files:
        ext = os.path.splitext(filepath)[1].lower()
        try:
            if ext == '.xlsx':
                df = _load_excel(filepath)
            elif ext == '.csv':
                df = _load_csv(filepath)
            elif ext == '.json':
                df = _load_json(filepath)
            else:
                continue
            
            if df is not None and not df.empty:
                all_dfs.append(df)
                print(f"  Loaded {len(df)} leads from {os.path.basename(filepath)}")

        except Exception as e:
            print(f"  Warning: Could not load {os.path.basename(filepath)}: {e}")

    if not all_dfs:
        raise ValueError("No valid data found in any file.")

    combined = pd.concat(all_dfs, ignore_index=True)
    return combined


def _load_excel(filepath):
    """Load an Excel file, handling the merged-header format."""
    # First, try to detect the header structure
    df_raw = pd.read_excel(filepath, header=None, nrows=5)

    # Check if row 0 has category headers (sparse) and row 1 has sub-headers (dense)
    row0_filled = df_raw.iloc[0].notna().sum()
    row1_filled = df_raw.iloc[1].notna().sum() if len(df_raw) > 1 else 0
    
    if row1_filled > row0_filled:
        # Merged-header format: use row 1 as headers, data starts at row 2
        df = pd.read_excel(filepath, header=1)
    else:
        # Standard format: use row 0 as headers
        df = pd.read_excel(filepath, header=0)

    # Drop rows where all values are NaN
    df = df.dropna(how='all')
    
    # Drop unnamed columns that are entirely empty
    unnamed_cols = [c for c in df.columns if str(c).startswith('Unnamed')]
    for col in unnamed_cols:
        if df[col].notna().sum() == 0:
            df = df.drop(col, axis=1)

    return df


def _load_csv(filepath):
    """Load a CSV file."""
    # Try different encodings
    for encoding in ['utf-8', 'latin-1', 'cp1252']:
        try:
            df = pd.read_csv(filepath, encoding=encoding)
            df = df.dropna(how='all')
            return df
        except (UnicodeDecodeError, pd.errors.ParserError):
            continue
    return None


def _load_json(filepath):
    """Load a JSON file."""
    df = pd.read_json(filepath, encoding='utf-8')
    df = df.dropna(how='all')
    return df


def get_file_info(folder_path=None):
    """Get information about available data files."""
    if folder_path is None:
        folder_path = DEFAULT_LEADS_PATH
    
    info = []
    patterns = ['*.xlsx', '*.csv', '*.json']
    for pattern in patterns:
        for filepath in glob.glob(os.path.join(folder_path, pattern)):
            if not os.path.basename(filepath).startswith('.~lock'):
                size = os.path.getsize(filepath)
                info.append({
                    'filename': os.path.basename(filepath),
                    'format': os.path.splitext(filepath)[1],
                    'size_kb': round(size / 1024, 1),
                    'path': filepath
                })
    return info

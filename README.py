def check_variant_ean_hsn(df):
    import pandas as pd
    from sshtunnel import SSHTunnelForwarder
    import psycopg2

    # Initialize log list
    log = []

    # Initialize a boolean Series to track errors
    error_mask = pd.Series(False, index=df.index)

    # Column mappings
    column_mappings = {
        'Variant Article Number': 'article_code',
        'EAN Number': 'ean',
        'HSN Code': 'hsn'
    }

    # 1. Check if required columns exist in the DataFrame
    missing_columns = [col for col in column_mappings.keys() if col not in df.columns]
    if missing_columns:
        log.append(f"Error: Missing columns in DataFrame: {', '.join(missing_columns)}.")
        return error_mask, log

    # 2. Establish SSH Tunnel to the Costing Database
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USERNAME,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(PROCURO_HOST, PROCURO_PORT)
        ) as tunnel:
            # 3. Connect to the Costing Database via the SSH Tunnel
            conn = psycopg2.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                database=PROCURO_DATABASE,
                user=PROCURO_USERNAME,
                password=PROCURO_PASSWORD
            )
            cursor = conn.cursor()

            # 4. Fetch valid combinations of article_code, ean, and hsn from the "article" table
            cursor.execute("SELECT article_code, ean, hsn FROM article WHERE article_code IS NOT NULL AND ean IS NOT NULL AND hsn IS NOT NULL;")
            fetched_rows = cursor.fetchall()
            valid_combinations = set((str(row[0]).strip(), str(row[1]).strip(), str(row[2]).strip()) for row in fetched_rows)

            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return error_mask, log

    # 5. Null Check for each column
    for excel_col in column_mappings.keys():
        null_mask = df[excel_col].isnull()
        null_count = null_mask.sum()
        if null_mask.any():
            log.append(f"Null values {null_count} found in '{excel_col}'.")
            error_mask = error_mask | null_mask

    # 6. Prepare DataFrame for Combination Matching
    # Strip and fill NaN with empty strings for comparison
    df['Variant Article Number_clean'] = df['Variant Article Number'].fillna('').astype(str).str.strip()
    df['EAN Number_clean'] = df['EAN Number'].fillna('').astype(str).str.strip()
    df['HSN Code_clean'] = df['HSN Code'].fillna('').astype(str).str.strip()

    # Combine cleaned columns into tuples for comparison
    df['Combined_Keys'] = list(zip(
        df['Variant Article Number_clean'],
        df['EAN Number_clean'],
        df['HSN Code_clean']
    ))

    # 7. Check for mismatched combinations
    mismatch_mask = ~df['Combined_Keys'].isin(valid_combinations)
    mismatch_count = mismatch_mask.sum()
    if mismatch_mask.any():
        log.append(f"{mismatch_count} rows have mismatched combinations of 'Variant Article Number', 'EAN Number', and 'HSN Code'.")
        error_mask = error_mask | mismatch_mask

    # Cleanup temporary columns
    df.drop(columns=['Variant Article Number_clean', 'EAN Number_clean', 'HSN Code_clean', 'Combined_Keys'], inplace=True)

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found in 'Variant Article Number', 'EAN Number', and 'HSN Code'.")

    return error_mask, log

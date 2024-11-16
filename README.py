def check_store_code_and_dc_code(df):
    import pandas as pd
    from sshtunnel import SSHTunnelForwarder
    import psycopg2

    # Initialize log list
    log = []

    # Initialize a boolean Series to track errors
    error_mask = pd.Series(False, index=df.index)

    # Column name
    store_code_column = 'Store Code'

    # 1. Check if required column exists in the DataFrame
    if store_code_column not in df.columns:
        log.append(f"Error: Missing column in DataFrame: '{store_code_column}'.")
        return error_mask, log

    # 2. Establish SSH Tunnel to the Procuro Database
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USERNAME,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(PROCURO_HOST, PROCURO_PORT)
        ) as tunnel:
            # 3. Connect to the Procuro Database via the SSH Tunnel
            conn = psycopg2.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                database=PROCURO_DATABASE,
                user=PROCURO_USERNAME,
                password=PROCURO_PASSWORD
            )
            cursor = conn.cursor()

            # 4. Fetch valid site_code and corresponding dc_code from the database
            cursor.execute("SELECT site_code, dc_code FROM site WHERE site_code IS NOT NULL;")
            fetched_data = cursor.fetchall()
            site_data = {
                str(row[0]).strip(): row[1] for row in fetched_data
            }

            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return error_mask, log

    # 5. Null Check for Store Code
    null_mask = df[store_code_column].isnull()
    null_count = null_mask.sum()
    if null_mask.any():
        log.append(f"Null values {null_count} found in '{store_code_column}'.")
        error_mask = error_mask | null_mask

    # 6. Check for invalid Store Codes
    df['Store Code_clean'] = df[store_code_column].fillna('').str.strip()
    invalid_mask = df['Store Code_clean'].apply(
        lambda code: code not in site_data or site_data[code] is None
    )
    invalid_count = invalid_mask.sum()
    if invalid_mask.any():
        log.append(f"{invalid_count} rows have invalid 'Store Code' or missing 'dc_code' in the database.")
        error_mask = error_mask | invalid_mask

    # Cleanup temporary columns
    df.drop(columns=['Store Code_clean'], inplace=True)

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'Store Code'.")

    return error_mask, log


def check_qty_positive(df):
    import pandas as pd

    # Initialize log list
    log = []

    # Column name
    qty_column = 'Qty'

    # 1. Check if the required column exists in the DataFrame
    if qty_column not in df.columns:
        log.append(f"Error: Missing column in DataFrame: '{qty_column}'.")
        return pd.Series(False, index=df.index), log

    # 2. Validate that Qty > 0
    invalid_mask = df[qty_column].fillna(0) <= 0
    invalid_count = invalid_mask.sum()

    if invalid_mask.any():
        log.append(f"{invalid_count} rows have 'Qty' less than or equal to 0.")

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'Qty'.")

    return invalid_mask, log


def check_uom_ea(df):
    import pandas as pd

    # Initialize log list
    log = []

    # Column name
    uom_column = 'UOM'

    # 1. Check if the required column exists in the DataFrame
    if uom_column not in df.columns:
        log.append(f"Error: Missing column in DataFrame: '{uom_column}'.")
        return pd.Series(False, index=df.index), log

    # 2. Validate that UOM is 'EA' (case-insensitive)
    invalid_mask = df[uom_column].fillna('').str.strip().str.upper() != 'EA'
    invalid_count = invalid_mask.sum()

    if invalid_mask.any():
        log.append(f"{invalid_count} rows have 'UOM' not equal to 'EA'.")

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'UOM'.")

    return invalid_mask, log

def check_priority_valid(df):
    import pandas as pd

    # Initialize log list
    log = []

    # Column name
    priority_column = 'Priority'

    # 1. Check if the required column exists in the DataFrame
    if priority_column not in df.columns:
        log.append(f"Error: Missing column in DataFrame: '{priority_column}'.")
        return pd.Series(False, index=df.index), log

    # 2. Define valid priority values
    valid_priorities = ['P1', 'P2', 'P3']

    # 3. Validate that Priority is one of 'P1', 'P2', or 'P3' (case-insensitive)
    invalid_mask = ~df[priority_column].fillna('').str.strip().str.upper().isin(valid_priorities)
    invalid_count = invalid_mask.sum()

    if invalid_mask.any():
        log.append(f"{invalid_count} rows have invalid 'Priority'. Only 'P1', 'P2', or 'P3' are allowed.")

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'Priority'.")

    return invalid_mask, log


def check_style_id_in_db(df):
    import pandas as pd
    from sshtunnel import SSHTunnelForwarder
    import psycopg2

    # Initialize log list
    log = []

    # Column name for Style Id
    style_id_column = 'Style Id'

    # 1. Check if the required column exists in the DataFrame
    if style_id_column not in df.columns:
        log.append(f"Error: Missing column in DataFrame: '{style_id_column}'.")
        return pd.Series(False, index=df.index), log

    # Initialize error mask
    error_mask = pd.Series(False, index=df.index)

    # 2. Establish SSH Tunnel to the Procuro Database
    try:
        with SSHTunnelForwarder(
            (SSH_HOST, SSH_PORT),
            ssh_username=SSH_USERNAME,
            ssh_pkey=SSH_KEY_PATH,
            remote_bind_address=(PROCURO_HOST, PROCURO_PORT)
        ) as tunnel:
            # 3. Connect to the Procuro Database via the SSH Tunnel
            conn = psycopg2.connect(
                host='127.0.0.1',
                port=tunnel.local_bind_port,
                database=PROCURO_DATABASE,
                user=PROCURO_USERNAME,
                password=PROCURO_PASSWORD
            )
            cursor = conn.cursor()

            # 4. Fetch valid style_codes from the COSTING table
            cursor.execute("SELECT style_code FROM costing WHERE style_code IS NOT NULL;")
            valid_style_codes = set(row[0].strip() for row in cursor.fetchall())

            # 5. Fetch matching style_id and status from the quotation table
            cursor.execute("""
                SELECT style_id, status 
                FROM quotation 
                WHERE status IS NOT NULL;
            """)
            quotation_data = {
                row[0]: row[1].strip().lower() for row in cursor.fetchall()
            }

            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return error_mask, log

    # 6. Check for null Style Id values in Excel
    null_mask = df[style_id_column].isnull()
    null_count = null_mask.sum()
    if null_mask.any():
        log.append(f"Null values {null_count} found in '{style_id_column}'.")
        error_mask = error_mask | null_mask

    # 7. Check if Style Id exists in the COSTING table
    df['Style Id_clean'] = df[style_id_column].str.strip()
    invalid_style_mask = ~df['Style Id_clean'].isin(valid_style_codes)
    invalid_style_count = invalid_style_mask.sum()
    if invalid_style_mask.any():
        log.append(f"{invalid_style_count} rows have 'Style Id' not found in COSTING table.")
        error_mask = error_mask | invalid_style_mask

    # 8. Check if Style Id matches quotation table and status is 'approved'
    valid_style_approved_mask = df['Style Id_clean'].apply(
        lambda style_id: style_id in quotation_data and quotation_data[style_id] == 'approved'
    )
    valid_style_approved_count = valid_style_approved_mask.sum()
    if valid_style_approved_mask.any():
        log.append(f"{valid_style_approved_count} rows have 'Style Id' matching 'approved' status in quotation table.")
        error_mask = error_mask | ~valid_style_approved_mask

    # Clean up temporary columns
    df.drop(columns=['Style Id_clean'], inplace=True)

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'Style Id'.")

    return error_mask, log

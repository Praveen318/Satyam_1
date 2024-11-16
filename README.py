def check_otb_purchase_group_and_freight_cost(df):
    import pandas as pd
    from sshtunnel import SSHTunnelForwarder
    import psycopg2

    # Initialize log list
    log = []

    # Initialize a boolean Series to track errors
    error_mask = pd.Series(False, index=df.index)

    # Column names
    otb_column = 'OTB Number'
    purchase_group_column = 'Purchase Group'

    # 1. Check if required columns exist in the DataFrame
    missing_columns = [col for col in [otb_column, purchase_group_column] if col not in df.columns]
    if missing_columns:
        log.append(f"Error: Missing columns in DataFrame: {', '.join(missing_columns)}.")
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

            # 4. Fetch valid purchase groups and their freight costs
            cursor.execute("SELECT purchase_group, freight_cost FROM purchase_group;")
            fetched_data = cursor.fetchall()
            purchase_group_data = {
                str(row[0]).strip().lower(): row[1] for row in fetched_data
            }

            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return error_mask, log

    # 5. Null and Mismatch Checks
    # Normalize the purchase group column from the Excel file
    df['Purchase Group_clean'] = df[purchase_group_column].fillna('').str.strip().str.lower()

    # Extract the last three characters of OTB Number and check against Purchase Group
    df['OTB_last3'] = df[otb_column].fillna('').astype(str).str[-3:].str.lower()

    # Check for mismatches
    otb_mismatch_mask = df['OTB_last3'] != df['Purchase Group_clean']
    otb_mismatch_count = otb_mismatch_mask.sum()
    if otb_mismatch_mask.any():
        log.append(f"{otb_mismatch_count} rows have mismatched 'OTB Number' and 'Purchase Group'.")
        error_mask = error_mask | otb_mismatch_mask

    # Check for null or invalid freight costs in the database
    invalid_freight_mask = df['Purchase Group_clean'].apply(
        lambda pg: pg not in purchase_group_data or purchase_group_data[pg] is None
    )
    invalid_freight_count = invalid_freight_mask.sum()
    if invalid_freight_mask.any():
        log.append(f"{invalid_freight_count} rows have null or invalid freight costs for the 'Purchase Group'.")
        error_mask = error_mask | invalid_freight_mask

    # Cleanup temporary columns
    df.drop(columns=['Purchase Group_clean', 'OTB_last3'], inplace=True)

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found for 'OTB Number' and 'Purchase Group'.")

    return error_mask, log

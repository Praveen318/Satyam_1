def check_fashion_grade_code(df):
    import pandas as pd
    from sshtunnel import SSHTunnelForwarder
    import psycopg2
    
    # Initialize log list
    log = []
    
    # ----------------------
    # Validation Steps
    # ----------------------
    # Initialize a boolean Series to track errors
    fashion_grade_code_error_mask = pd.Series(False, index=df.index)
    
    # 1. Check if 'Fashion Grade Code' column exists
    if 'Fashion Grade Code' not in df.columns:
        log.append("Error: DataFrame does not contain a 'Fashion Grade Code' column.")
        return fashion_grade_code_error_mask, log
    
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
            
            # 4. Fetch Unique Valid fashion_grade_code from the 'fashion grade' Table
            cursor.execute("SELECT DISTINCT code FROM fashion_grade WHERE code IS NOT NULL;")
            fetched_codes = cursor.fetchall()
            # Create a set of valid fashion_grade_code for comparison
            valid_fashion_grade_codes = set(int(code[0]) for code in fetched_codes if code[0].isdigit())
            
            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return fashion_grade_code_error_mask, log

    # 5. Identify Null Entries in 'Fashion Grade Code' Column
    null_mask = df['Fashion Grade Code'].isnull()
    null_count = null_mask.sum()
    if null_mask.any():
        log.append(f"Null values {null_count} found in 'Fashion Grade Code'.")
        fashion_grade_code_error_mask = fashion_grade_code_error_mask | null_mask
    
    # 6. Convert 'Fashion Grade Code' to numeric for validation
    df['Fashion Grade Code_numeric'] = pd.to_numeric(df['Fashion Grade Code'], errors='coerce')
    
    # Identify entries that are not in the valid set
    invalid_mask = ~df['Fashion Grade Code_numeric'].isin(valid_fashion_grade_codes) & df['Fashion Grade Code_numeric'].notnull()
    invalid_count = invalid_mask.sum()
    if invalid_mask.any():
        log.append(f"Invalid Fashion Grade Code {invalid_count} found in 'Fashion Grade Code'.")
        fashion_grade_code_error_mask = fashion_grade_code_error_mask | invalid_mask
    
    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found in Fashion Grade Code")
    
    # Cleanup temporary column
    df.drop(columns=['Fashion Grade Code_numeric'], inplace=True)
    
    return fashion_grade_code_error_mask, log

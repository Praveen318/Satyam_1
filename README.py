def check_fashion_grade_description(df):
        
    # Initialize log list
    log = []
    
    # ----------------------
    # Validation Steps
    # ----------------------
    # Initialize a boolean Series to track errors
    fashion_grade_description_error_mask = pd.Series(False, index=df.index)
    # 1. Check if 'Fashion grade description' column exists
    if 'Fashion Grade Description' not in df.columns:
        log.append("Error: DataFrame does not contain a 'Fashion Grade Description' column.")
        return fashion_grade_description_error_mask, log
    
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
            
            # 4. Fetch Unique Valid fashion_grade_description from the 'fashion grade' Table
            cursor.execute("SELECT DISTINCT fashion_grade FROM fashion_grade WHERE fashion_grade IS NOT NULL;")
            fetched_fashion_grades = cursor.fetchall()
            # Create a set of valid fashion_grade_description in lowercase for case-insensitive comparison
            valid_fashion_grade_description = set(fashion_grade[0].strip().lower() for fashion_grade in fetched_fashion_grades if fashion_grade[0])
            
            cursor.close()
            conn.close()
    except Exception as e:
        log.append(f"Database connection error: {e}")
        return df, log

    
    # 5. Identify Null Entries in 'fashion_grade_description' Column
    null_mask = df['Fashion Grade Description'].isnull()
    null_count = null_mask.sum()
    if null_mask.any():
        log.append(f"Null values {null_count} found in 'Fashion Grade Description'.")
        fashion_grade_description_error_mask= fashion_grade_description_error_mask | null_mask
    
    # 6. Identify Invalid Fashion Grade Description Entries (Case-Insensitive)
    # Convert all Fashion Grade Description entries to lowercase for comparison
    df['Fashion Grade Description_lower'] = df['Fashion Grade Description'].str.lower().str.strip()
    invalid_mask = ~df['Fashion Grade Description_lower'].isin(valid_fashion_grade_description) & df['Fashion Grade Description'].notnull()
    invalid_count = invalid_mask.sum()
    if invalid_mask.any():
        log.append(f"Invalid Fashion Grade Description {invalid_count} found in 'Fashion Grade Description_lower'.")
        fashion_grade_description_error_mask = fashion_grade_description_error_mask | invalid_mask
    

    # If no errors were found, log accordingly
    if not log:
        log.append("- no validation error found in Fashion Grade Description")
    

    df.drop(columns=['Fashion Grade Description_lower'], inplace=True)
    
    return fashion_grade_description_error_mask, log

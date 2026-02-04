def analyze_top_procedures(chunk_df, year, chunk_idx):
    """
    Analyze top 50 most frequent CPT codes and their RVU components.
    Call this after the RVU merge.
    """
    if 'PROC1' not in chunk_df.columns:
        return None
    
    # Count frequency of each CPT code
    proc_counts = chunk_df['PROC1'].value_counts().head(50).reset_index()
    proc_counts.columns = ['PROC1', 'frequency']
    
    # Get RVU values for these top 50 codes
    # Take first value for each PROC1 (should be same within a chunk)
    rvu_cols = ['DESC', 'WORK_RVU', 'FACILITY_PE_RVU', 'NON-FAC_PE_RVU', 'MP_RVU']
    available_rvu_cols = [col for col in rvu_cols if col in chunk_df.columns]
    
    if len(available_rvu_cols) > 0:
        # Get unique PROC1 with their RVU values
        proc_rvu = chunk_df.groupby('PROC1')[available_rvu_cols].first().reset_index()
        
        # Merge with counts
        top_procedures = proc_counts.merge(proc_rvu, on='PROC1', how='left')
        
        # Calculate total RVU (using non-facility PE as default)
        if all(col in top_procedures.columns for col in ['WORK_RVU', 'NON-FAC_PE_RVU', 'MP_RVU']):
            top_procedures['TOTAL_RVU_NONFAC'] = (
                pd.to_numeric(top_procedures['WORK_RVU'], errors='coerce').fillna(0) +
                pd.to_numeric(top_procedures['NON-FAC_PE_RVU'], errors='coerce').fillna(0) +
                pd.to_numeric(top_procedures['MP_RVU'], errors='coerce').fillna(0)
            )
        
        # Round RVU values for cleaner display (but not DESC)
        numeric_cols = [col for col in available_rvu_cols + ['TOTAL_RVU_NONFAC'] if col != 'DESC']
        for col in numeric_cols:
            if col in top_procedures.columns:
                top_procedures[col] = pd.to_numeric(top_procedures[col], errors='coerce').round(2)
        
        # Reorder columns to put DESC right after PROC1
        cols = ['PROC1', 'frequency']
        if 'DESC' in top_procedures.columns:
            cols.append('DESC')
        # Add remaining RVU columns
        for col in top_procedures.columns:
            if col not in cols:
                cols.append(col)
        top_procedures = top_procedures[cols]
        
        return top_procedures
    
    return proc_counts

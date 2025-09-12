-- Migration script to rename PDF_Workflow_Tracking to Excel_Workflow_Tracking
-- and update all field names from PDF to Excel

-- Check if old table exists and new table doesn't exist
IF EXISTS (SELECT * FROM sysobjects WHERE name='PDF_Workflow_Tracking' AND xtype='U')
   AND NOT EXISTS (SELECT * FROM sysobjects WHERE name='Excel_Workflow_Tracking' AND xtype='U')
BEGIN
    -- Create new Excel_Workflow_Tracking table with updated field names
    CREATE TABLE ref.Excel_Workflow_Tracking (
        workflow_id VARCHAR(50) PRIMARY KEY,
        csv_file_path NVARCHAR(500) NOT NULL,
        excel_file_path NVARCHAR(500),
        status VARCHAR(50) NOT NULL,
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        excel_generated_at DATETIME2,
        approved_at DATETIME2,
        completed_at DATETIME2,
        error_message NVARCHAR(MAX),
        user_config NVARCHAR(MAX),
        processed_by NVARCHAR(100),
        retry_count INT DEFAULT 0
    );
    
    -- Migrate data from old table to new table, updating state names
    INSERT INTO ref.Excel_Workflow_Tracking (
        workflow_id, csv_file_path, excel_file_path, status, created_at, 
        excel_generated_at, approved_at, completed_at, error_message, 
        user_config, processed_by, retry_count
    )
    SELECT 
        workflow_id, 
        csv_file_path, 
        pdf_file_path, -- Maps old pdf_file_path to new excel_file_path
        CASE 
            WHEN status = 'pending_pdf' THEN 'pending_excel'
            WHEN status = 'pdf_generated' THEN 'excel_generated'
            ELSE status
        END as status, -- Update state names
        created_at, 
        pdf_generated_at, -- Maps old pdf_generated_at to new excel_generated_at
        approved_at, 
        completed_at, 
        error_message, 
        user_config, 
        processed_by, 
        retry_count
    FROM ref.PDF_Workflow_Tracking;
    
    -- Drop the old table
    DROP TABLE ref.PDF_Workflow_Tracking;
    
    PRINT 'Successfully migrated PDF_Workflow_Tracking to Excel_Workflow_Tracking';
END
ELSE
BEGIN
    IF EXISTS (SELECT * FROM sysobjects WHERE name='Excel_Workflow_Tracking' AND xtype='U')
        PRINT 'Excel_Workflow_Tracking table already exists - migration not needed';
    ELSE
        PRINT 'PDF_Workflow_Tracking table not found - creating new Excel_Workflow_Tracking table';
END
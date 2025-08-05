-- CSV数据管理系统 - 数据库初始化脚本 (Windows认证)
-- 版本: 1.0  
-- 日期: 2025-08-04
-- 认证方式: Windows Authentication

USE master;
GO

-- 创建数据库
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'CSVDataManager')
BEGIN
    CREATE DATABASE CSVDataManager
    COLLATE Chinese_PRC_CI_AS;
END
GO

USE CSVDataManager;
GO

-- 1. 用户组表
CREATE TABLE sys_user_groups (
    group_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    group_name NVARCHAR(50) NOT NULL UNIQUE,
    permissions NVARCHAR(MAX), -- JSON格式权限配置
    description NVARCHAR(255),
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1
);

-- 2. 用户表
CREATE TABLE sys_users (
    user_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    username NVARCHAR(50) UNIQUE NOT NULL,
    email NVARCHAR(100) UNIQUE NOT NULL,
    password_hash NVARCHAR(255) NOT NULL,
    user_group_id NVARCHAR(36),
    full_name NVARCHAR(100),
    last_login DATETIME2,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1,
    CONSTRAINT FK_Users_UserGroups FOREIGN KEY (user_group_id) 
        REFERENCES sys_user_groups(group_id)
);

-- 3. 系统配置表
CREATE TABLE sys_config (
    config_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    csv_filename_pattern NVARCHAR(255) NOT NULL,
    validation_rule NVARCHAR(500),
    validation_type NVARCHAR(20) CHECK (validation_type IN ('stored_procedure', 'python_class')),
    target_table_name NVARCHAR(128) NOT NULL,
    stage_table_name NVARCHAR(128) NOT NULL,
    start_date DATETIME2,
    end_date DATETIME2,
    created_by NVARCHAR(36) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE(),
    updated_at DATETIME2 DEFAULT GETDATE(),
    is_active BIT DEFAULT 1,
    CONSTRAINT FK_Config_Users FOREIGN KEY (created_by) 
        REFERENCES sys_users(user_id)
);

-- 4. 验证结果表
CREATE TABLE validation_results (
    validation_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    csv_filename NVARCHAR(255) NOT NULL,
    config_id NVARCHAR(36),
    validation_rule NVARCHAR(500),
    start_time DATETIME2 NOT NULL DEFAULT GETDATE(),
    end_time DATETIME2,
    status NVARCHAR(20) DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'RUNNING', 'SUCCESS', 'FAILED')),
    total_records INT DEFAULT 0,
    valid_records INT DEFAULT 0,
    invalid_records INT DEFAULT 0,
    error_details NVARCHAR(MAX), -- JSON格式错误详情
    created_by NVARCHAR(36),
    processing_time_ms INT,
    CONSTRAINT FK_ValidationResults_Config FOREIGN KEY (config_id) 
        REFERENCES sys_config(config_id),
    CONSTRAINT FK_ValidationResults_Users FOREIGN KEY (created_by) 
        REFERENCES sys_users(user_id)
);

-- 5. 系统日志表
CREATE TABLE sys_logs (
    log_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    operation_type NVARCHAR(50) NOT NULL,
    user_id NVARCHAR(36),
    timestamp DATETIME2 DEFAULT GETDATE(),
    details NVARCHAR(MAX), -- JSON格式详情
    status NVARCHAR(20) DEFAULT 'SUCCESS' CHECK (status IN ('SUCCESS', 'FAILED', 'WARNING')),
    ip_address NVARCHAR(45),
    user_agent NVARCHAR(500),
    resource_type NVARCHAR(50), -- 操作的资源类型
    resource_id NVARCHAR(36),   -- 操作的资源ID
    CONSTRAINT FK_Logs_Users FOREIGN KEY (user_id) 
        REFERENCES sys_users(user_id)
);

-- 6. 文件处理记录表
CREATE TABLE file_processing_records (
    record_id NVARCHAR(36) PRIMARY KEY DEFAULT NEWID(),
    original_filename NVARCHAR(255) NOT NULL,
    file_size BIGINT,
    file_hash NVARCHAR(64), -- MD5或SHA256文件哈希
    upload_time DATETIME2 DEFAULT GETDATE(),
    processing_start_time DATETIME2,
    processing_end_time DATETIME2,
    status NVARCHAR(20) DEFAULT 'UPLOADED' CHECK (status IN ('UPLOADED', 'PROCESSING', 'COMPLETED', 'FAILED')),
    target_table NVARCHAR(128),
    stage_table NVARCHAR(128),
    total_rows INT DEFAULT 0,
    imported_rows INT DEFAULT 0,
    error_rows INT DEFAULT 0,
    validation_id NVARCHAR(36),
    created_by NVARCHAR(36),
    error_message NVARCHAR(MAX),
    CONSTRAINT FK_FileRecords_Validation FOREIGN KEY (validation_id) 
        REFERENCES validation_results(validation_id),
    CONSTRAINT FK_FileRecords_Users FOREIGN KEY (created_by) 
        REFERENCES sys_users(user_id)
);

-- 创建索引
-- 用户表索引
CREATE INDEX IX_Users_Username ON sys_users(username);
CREATE INDEX IX_Users_Email ON sys_users(email);
CREATE INDEX IX_Users_UserGroup ON sys_users(user_group_id);

-- 配置表索引
CREATE INDEX IX_Config_Pattern ON sys_config(csv_filename_pattern);
CREATE INDEX IX_Config_CreatedBy ON sys_config(created_by);
CREATE INDEX IX_Config_Active ON sys_config(is_active);

-- 验证结果表索引
CREATE INDEX IX_ValidationResults_Filename ON validation_results(csv_filename);
CREATE INDEX IX_ValidationResults_Status ON validation_results(status);
CREATE INDEX IX_ValidationResults_StartTime ON validation_results(start_time);
CREATE INDEX IX_ValidationResults_Config ON validation_results(config_id);

-- 系统日志表索引
CREATE INDEX IX_Logs_Timestamp ON sys_logs(timestamp);
CREATE INDEX IX_Logs_UserId ON sys_logs(user_id);
CREATE INDEX IX_Logs_OperationType ON sys_logs(operation_type);
CREATE INDEX IX_Logs_Status ON sys_logs(status);

-- 文件处理记录表索引
CREATE INDEX IX_FileRecords_Filename ON file_processing_records(original_filename);
CREATE INDEX IX_FileRecords_Status ON file_processing_records(status);
CREATE INDEX IX_FileRecords_UploadTime ON file_processing_records(upload_time);
CREATE INDEX IX_FileRecords_CreatedBy ON file_processing_records(created_by);

-- 插入初始数据
-- 创建默认用户组
INSERT INTO sys_user_groups (group_id, group_name, permissions, description) VALUES
('admin-group-001', '系统管理员', '{"all": true}', '拥有所有系统权限'),
('data-manager-002', '数据管理员', '{"configs": ["create", "read", "update"], "files": ["upload", "import"], "logs": ["read"]}', '负责数据配置和导入'),
('business-user-003', '业务用户', '{"logs": ["read"], "reports": ["read"]}', '只能查看日志和报表');

-- 创建默认管理员用户 (密码: admin123，实际使用时应该是加密后的哈希值)
INSERT INTO sys_users (user_id, username, email, password_hash, user_group_id, full_name) VALUES
('admin-user-001', 'admin', 'admin@company.com', '$2b$12$LQv3c1yqBWVHxkd0LHAkCOYz6TtxMQJqhN8/LewdBPj2SFN5Q6LRi', 'admin-group-001', '系统管理员');

-- 创建示例配置
INSERT INTO sys_config (config_id, csv_filename_pattern, validation_rule, validation_type, target_table_name, stage_table_name, created_by) VALUES
('config-001', 'customer.*.csv', 'sp_validate_customer_data', 'stored_procedure', 'customer_data', 'customer_data_stage', 'admin-user-001'),
('config-002', 'product.*.csv', 'ProductDataValidator', 'python_class', 'product_data', 'product_data_stage', 'admin-user-001');

-- 创建存储过程：动态创建业务表
GO
CREATE PROCEDURE sp_create_business_table
    @table_name NVARCHAR(128),
    @columns NVARCHAR(MAX), -- JSON格式的列定义
    @create_stage_table BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @stage_table_name NVARCHAR(128) = @table_name + '_stage';
    
    -- 创建主表
    SET @sql = N'
    CREATE TABLE [' + @table_name + '] (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        sequence_number INT NOT NULL DEFAULT 1,
        load_timestamp DATETIME2 DEFAULT GETDATE(),
        created_at DATETIME2 DEFAULT GETDATE(),
        updated_at DATETIME2 DEFAULT GETDATE()';
    
    -- 这里需要根据传入的列信息动态添加列
    -- 简化版本：假设所有列都是NVARCHAR(MAX)
    DECLARE @col_count INT = 1;
    WHILE @col_count <= 20  -- 假设最多20列
    BEGIN
        SET @sql = @sql + ',
        col_' + CAST(@col_count AS NVARCHAR(2)) + ' NVARCHAR(MAX)';
        SET @col_count = @col_count + 1;
    END
    
    SET @sql = @sql + '
    );';
    
    -- 执行创建主表
    EXEC sp_executesql @sql;
    
    -- 创建对应的索引
    SET @sql = N'CREATE INDEX IX_' + @table_name + '_Sequence ON [' + @table_name + '](sequence_number);';
    EXEC sp_executesql @sql;
    
    SET @sql = N'CREATE INDEX IX_' + @table_name + '_LoadTime ON [' + @table_name + '](load_timestamp);';
    EXEC sp_executesql @sql;
    
    -- 创建Stage表（如果需要）
    IF @create_stage_table = 1
    BEGIN
        SET @sql = N'
        CREATE TABLE [' + @stage_table_name + '] (
            stage_id BIGINT IDENTITY(1,1) PRIMARY KEY,
            validation_id NVARCHAR(36),
            validation_status NVARCHAR(20) DEFAULT ''PENDING'',
            validation_errors NVARCHAR(MAX),
            sequence_number INT NOT NULL DEFAULT 1,
            load_timestamp DATETIME2 DEFAULT GETDATE(),
            created_at DATETIME2 DEFAULT GETDATE()';
        
        -- 添加同样的业务列
        SET @col_count = 1;
        WHILE @col_count <= 20
        BEGIN
            SET @sql = @sql + ',
            col_' + CAST(@col_count AS NVARCHAR(2)) + ' NVARCHAR(MAX)';
            SET @col_count = @col_count + 1;
        END
        
        SET @sql = @sql + '
        );';
        
        -- 执行创建Stage表
        EXEC sp_executesql @sql;
        
        -- 创建Stage表索引
        SET @sql = N'CREATE INDEX IX_' + @stage_table_name + '_ValidationId ON [' + @stage_table_name + '](validation_id);';
        EXEC sp_executesql @sql;
        
        SET @sql = N'CREATE INDEX IX_' + @stage_table_name + '_Status ON [' + @stage_table_name + '](validation_status);';
        EXEC sp_executesql @sql;
    END
    
    PRINT '表创建成功: ' + @table_name;
    IF @create_stage_table = 1
        PRINT 'Stage表创建成功: ' + @stage_table_name;
END
GO

-- 创建示例验证存储过程
CREATE PROCEDURE sp_validate_customer_data
    @stage_table_name NVARCHAR(128),
    @validation_id NVARCHAR(36),
    @result_code INT OUTPUT,
    @error_message NVARCHAR(MAX) OUTPUT,
    @valid_count INT OUTPUT,
    @total_count INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @sql NVARCHAR(MAX);
    DECLARE @error_count INT = 0;
    
    -- 初始化输出参数
    SET @result_code = 0;
    SET @error_message = '';
    SET @valid_count = 0;
    SET @total_count = 0;
    
    BEGIN TRY
        -- 获取总记录数
        SET @sql = N'SELECT @total_count = COUNT(*) FROM [' + @stage_table_name + ']';
        EXEC sp_executesql @sql, N'@total_count INT OUTPUT', @total_count OUTPUT;
        
        -- 示例验证逻辑：检查邮箱格式
        SET @sql = N'
        UPDATE [' + @stage_table_name + '] 
        SET validation_status = ''INVALID'',
            validation_errors = ''邮箱格式不正确''
        WHERE col_2 IS NOT NULL 
        AND col_2 NOT LIKE ''%@%.%''
        AND validation_id = @validation_id';
        
        EXEC sp_executesql @sql, N'@validation_id NVARCHAR(36)', @validation_id;
        
        -- 标记有效记录
        SET @sql = N'
        UPDATE [' + @stage_table_name + '] 
        SET validation_status = ''VALID''
        WHERE validation_status = ''PENDING''
        AND validation_id = @validation_id';
        
        EXEC sp_executesql @sql, N'@validation_id NVARCHAR(36)', @validation_id;
        
        -- 获取有效记录数
        SET @sql = N'
        SELECT @valid_count = COUNT(*) 
        FROM [' + @stage_table_name + '] 
        WHERE validation_status = ''VALID'' 
        AND validation_id = @validation_id';
        
        EXEC sp_executesql @sql, N'@validation_id NVARCHAR(36), @valid_count INT OUTPUT', @validation_id, @valid_count OUTPUT;
        
        SET @result_code = 0; -- 成功
        
    END TRY
    BEGIN CATCH
        SET @result_code = 1; -- 失败
        SET @error_message = ERROR_MESSAGE();
    END CATCH
END
GO

-- 创建数据统计视图
CREATE VIEW v_data_import_statistics AS
SELECT 
    CAST(processing_start_time AS DATE) as import_date,
    COUNT(*) as total_files,
    SUM(total_rows) as total_rows,
    SUM(imported_rows) as successful_rows,
    SUM(error_rows) as error_rows,
    CAST(AVG(CAST(imported_rows AS FLOAT) / NULLIF(total_rows, 0) * 100) AS DECIMAL(5,2)) as success_rate_percent
FROM file_processing_records
WHERE processing_start_time IS NOT NULL
GROUP BY CAST(processing_start_time AS DATE);
GO

-- 创建用户活跃度统计视图
CREATE VIEW v_user_activity_statistics AS
SELECT 
    u.username,
    u.full_name,
    g.group_name,
    COUNT(l.log_id) as total_operations,
    MAX(l.timestamp) as last_activity,
    DATEDIFF(day, MAX(l.timestamp), GETDATE()) as days_since_last_activity
FROM sys_users u
LEFT JOIN sys_user_groups g ON u.user_group_id = g.group_id
LEFT JOIN sys_logs l ON u.user_id = l.user_id
WHERE u.is_active = 1
GROUP BY u.user_id, u.username, u.full_name, g.group_name;
GO

PRINT '数据库初始化完成！';
PRINT '默认管理员账户: admin / admin123';
PRINT '请及时修改默认密码！';
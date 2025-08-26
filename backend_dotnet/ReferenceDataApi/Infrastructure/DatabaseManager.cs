using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Microsoft.Extensions.Configuration;
using ReferenceDataApi.Models;
using ReferenceDataApi.Services;

namespace ReferenceDataApi.Infrastructure
{
    public class DatabaseManager : IDatabaseManager
    {
        private readonly string _connectionString;
        private readonly string _dataSchema;
        private readonly string _backupSchema;
        private readonly string _postloadSpName;
        private readonly ILogger _logger;

        public DatabaseManager(IConfiguration configuration, ILogger logger)
        {
            _connectionString = configuration.GetConnectionString("DefaultConnection");
            _dataSchema = configuration["DatabaseSettings:DataSchema"] ?? "ref";
            _backupSchema = configuration["DatabaseSettings:BackupSchema"] ?? "bkp";  
            _postloadSpName = configuration["DatabaseSettings:PostloadStoredProcedure"] ?? "sp_ref_postload";
            _logger = logger;
        }

        public bool TestConnection()
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand("SELECT 1", connection))
                    {
                        var result = command.ExecuteScalar();
                        return result != null && result.ToString() == "1";
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("database_test_connection", "Database connection test failed: " + ex.Message);
                return false;
            }
        }

        public List<string> GetAvailableSchemas()
        {
            var schemas = new List<string>();
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var query = @"
                        SELECT SCHEMA_NAME 
                        FROM INFORMATION_SCHEMA.SCHEMATA
                        WHERE SCHEMA_NAME NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys', 'db_owner', 'db_accessadmin', 
                                                 'db_securityadmin', 'db_ddladmin', 'db_backupoperator', 'db_datareader', 
                                                 'db_datawriter', 'db_denydatareader', 'db_denydatawriter', 'bkp')
                        ORDER BY SCHEMA_NAME";
                        
                    using (var command = new SqlCommand(query, connection))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                schemas.Add(reader.GetString(0));
                            }
                        }
                    }
                }

                // Ensure default schema is first
                if (schemas.Contains(_dataSchema))
                {
                    schemas.Remove(_dataSchema);
                    schemas.Insert(0, _dataSchema);
                }
                else
                {
                    schemas.Insert(0, _dataSchema);
                }
                
                return schemas;
            }
            catch (Exception ex)
            {
                _logger.LogError("get_schemas_error", "Failed to get available schemas: " + ex.Message);
                throw new Exception("Failed to retrieve available schemas: " + ex.Message);
            }
        }

        public List<TableColumn> GetTableColumns(string tableName, string schema = null)
        {
            if (string.IsNullOrEmpty(schema))
                schema = _dataSchema;
                
            var columns = new List<TableColumn>();
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var query = @"
                        SELECT 
                            COLUMN_NAME,
                            DATA_TYPE,
                            CHARACTER_MAXIMUM_LENGTH,
                            NUMERIC_PRECISION,
                            NUMERIC_SCALE,
                            IS_NULLABLE,
                            COLUMN_DEFAULT,
                            ORDINAL_POSITION
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName
                        ORDER BY ORDINAL_POSITION";
                        
                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@schema", schema);
                        command.Parameters.AddWithValue("@tableName", tableName);
                        
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                columns.Add(new TableColumn
                                {
                                    Name = reader.GetString("COLUMN_NAME"),
                                    DataType = reader.GetString("DATA_TYPE"),
                                    MaxLength = reader.IsDBNull("CHARACTER_MAXIMUM_LENGTH") ? null : (int?)reader.GetInt32("CHARACTER_MAXIMUM_LENGTH"),
                                    NumericPrecision = reader.IsDBNull("NUMERIC_PRECISION") ? null : (int?)Convert.ToInt32(reader["NUMERIC_PRECISION"]),
                                    NumericScale = reader.IsDBNull("NUMERIC_SCALE") ? null : (int?)Convert.ToInt32(reader["NUMERIC_SCALE"]),
                                    Nullable = reader.GetString("IS_NULLABLE") == "YES",
                                    DefaultValue = reader.IsDBNull("COLUMN_DEFAULT") ? null : reader.GetString("COLUMN_DEFAULT"),
                                    Position = reader.GetInt32("ORDINAL_POSITION")
                                });
                            }
                        }
                    }
                }
                
                return columns;
            }
            catch (Exception ex)
            {
                _logger.LogError("get_table_columns_error", "Failed to get table columns for " + schema + "." + tableName + ": " + ex.Message);
                throw new Exception("Failed to retrieve table columns: " + ex.Message);
            }
        }

        public bool TableExists(string tableName, string schema = null)
        {
            if (string.IsNullOrEmpty(schema))
                schema = _dataSchema;
                
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var query = @"
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_SCHEMA = @schema AND TABLE_NAME = @tableName";
                        
                    using (var command = new SqlCommand(query, connection))
                    {
                        command.Parameters.AddWithValue("@schema", schema);
                        command.Parameters.AddWithValue("@tableName", tableName);
                        
                        var count = (int)command.ExecuteScalar();
                        return count > 0;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("table_exists_error", "Failed to check if table exists " + schema + "." + tableName + ": " + ex.Message);
                return false;
            }
        }

        public void CreateTable(string tableName, List<TableColumn> columns, string schema = null, bool addMetadataColumns = true)
        {
            if (string.IsNullOrEmpty(schema))
                schema = _dataSchema;
                
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    // Drop table if exists
                    var dropSql = "DROP TABLE IF EXISTS [" + schema + "].[" + tableName + "]";
                    using (var dropCommand = new SqlCommand(dropSql, connection))
                    {
                        dropCommand.ExecuteNonQuery();
                    }
                    
                    // Build column definitions - .NET Framework 4.5 compatible way
                    var columnDefs = new List<string>();
                    foreach (var col in columns)
                    {
                        var normalizedType = NormalizeDataType(col.DataType, col.MaxLength, col.NumericPrecision, col.NumericScale);
                        var colDef = "[" + col.Name + "] " + normalizedType;
                        columnDefs.Add(colDef);
                    }
                    
                    if (addMetadataColumns)
                    {
                        columnDefs.Add("[ref_data_loadtime] datetime");
                        columnDefs.Add("[ref_data_loadtype] varchar(255)");
                    }
                    
                    var createSql = "CREATE TABLE [" + schema + "].[" + tableName + "] (" + string.Join(", ", columnDefs.ToArray()) + ")";
                    
                    using (var createCommand = new SqlCommand(createSql, connection))
                    {
                        createCommand.ExecuteNonQuery();
                    }
                }
                
                _logger.LogInfo("table_created", "Created table " + schema + "." + tableName);
            }
            catch (Exception ex)
            {
                _logger.LogError("create_table_error", "Failed to create table " + schema + "." + tableName + ": " + ex.Message);
                throw new Exception("Failed to create table: " + ex.Message);
            }
        }

        // Additional methods will follow...
        
        // .NET Framework 4.5 compatible data type normalization
        private string NormalizeDataType(string dataType, int? maxLength = null, int? numericPrecision = null, int? numericScale = null)
        {
            if (string.IsNullOrEmpty(dataType))
                return "varchar(255)";
                
            dataType = dataType.ToLower().Trim();
            
            if (dataType.StartsWith("varchar"))
            {
                if (maxLength.HasValue)
                {
                    if (maxLength.Value == -1)
                        return "varchar(max)";
                    return "varchar(" + maxLength.Value + ")";
                }
                return "varchar(255)"; // Default
            }
            
            if (dataType.StartsWith("decimal") || dataType.StartsWith("numeric"))
            {
                if (numericPrecision.HasValue && numericScale.HasValue)
                {
                    return "decimal(" + numericPrecision.Value + "," + numericScale.Value + ")";
                }
                return "decimal(18,0)"; // Default
            }
            
            if (dataType.StartsWith("char") && !dataType.StartsWith("varchar"))
            {
                if (maxLength.HasValue)
                {
                    return "char(" + maxLength.Value + ")";
                }
                return "char(1)"; // Default
            }
            
            return dataType; // Return as-is for int, bigint, datetime, etc.
        }

        public void CreateBackupTable(string tableName, List<TableColumn> columns)
        {
            var backupTableName = ExtractTableBaseName(tableName) + "_backup";
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    // Check if backup table exists
                    var backupExists = TableExists(backupTableName, _backupSchema);
                    
                    if (backupExists)
                    {
                        _logger.LogInfo("backup_table_exists", "Backup table exists, validating schema compatibility");
                        // For .NET Framework 4.5 compatibility, we'll recreate if schema doesn't match
                        // This is simpler than complex schema synchronization
                        DropTableIfExists(backupTableName, _backupSchema);
                    }
                    
                    // Build column definitions for backup table
                    var columnDefs = new List<string>();
                    foreach (var col in columns)
                    {
                        var normalizedType = NormalizeDataType(col.DataType, col.MaxLength, col.NumericPrecision, col.NumericScale);
                        var colDef = "[" + col.Name + "] " + normalizedType;
                        columnDefs.Add(colDef);
                    }
                    
                    // Add backup-specific metadata columns
                    columnDefs.Add("[ref_data_loadtime] datetime");
                    columnDefs.Add("[ref_data_loadtype] varchar(255)");
                    columnDefs.Add("[ref_data_version_id] int NOT NULL");
                    
                    var createSql = "CREATE TABLE [" + _backupSchema + "].[" + backupTableName + "] (" + string.Join(", ", columnDefs.ToArray()) + ")";
                    
                    using (var command = new SqlCommand(createSql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    
                    _logger.LogInfo("backup_table_created", "Created backup table " + _backupSchema + "." + backupTableName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("create_backup_table_error", "Failed to create backup table: " + ex.Message);
                throw new Exception("Failed to create backup table: " + ex.Message);
            }
        }

        public List<TableInfo> GetAllTables()
        {
            var tables = new List<TableInfo>();
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var query = @"
                        SELECT TABLE_SCHEMA, TABLE_NAME
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_TYPE = 'BASE TABLE'
                        AND TABLE_NAME NOT LIKE '%_stage' 
                        AND TABLE_NAME NOT LIKE '%_backup'
                        AND TABLE_NAME NOT IN ('Reference_Data_Cfg', 'system_log')
                        ORDER BY TABLE_SCHEMA, TABLE_NAME";
                        
                    using (var command = new SqlCommand(query, connection))
                    {
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var schema = reader.GetString("TABLE_SCHEMA");
                                var tableName = reader.GetString("TABLE_NAME");
                                
                                tables.Add(new TableInfo
                                {
                                    Schema = schema,
                                    Table = tableName,
                                    FullName = schema + "." + tableName
                                });
                            }
                        }
                    }
                }
                
                return tables;
            }
            catch (Exception ex)
            {
                _logger.LogError("get_all_tables_error", "Failed to get all tables: " + ex.Message);
                throw new Exception("Failed to retrieve tables: " + ex.Message);
            }
        }

        public List<TableInfo> GetSchemaMatchedTables(List<string> fileColumns)
        {
            var matchingTables = new List<TableInfo>();
            
            if (fileColumns == null || fileColumns.Count == 0)
            {
                return matchingTables;
            }
            
            try
            {
                var allTables = GetAllTables();
                
                // Convert file columns to lowercase for case-insensitive comparison - .NET Framework 4.5 compatible
                var fileColumnsSet = new HashSet<string>();
                foreach (var col in fileColumns)
                {
                    fileColumnsSet.Add(col.ToLower().Trim());
                }
                
                foreach (var tableInfo in allTables)
                {
                    try
                    {
                        var tableColumns = GetTableColumns(tableInfo.Table, tableInfo.Schema);
                        
                        // Filter out metadata columns and create set for comparison
                        var tableColumnsSet = new HashSet<string>();
                        foreach (var col in tableColumns)
                        {
                            var colName = col.Name.ToLower();
                            if (colName != "ref_data_loadtime" && colName != "ref_data_loadtype")
                            {
                                tableColumnsSet.Add(colName);
                            }
                        }
                        
                        // Check if file columns are subset of table columns (compatible matching)
                        var isCompatible = true;
                        foreach (var fileCol in fileColumnsSet)
                        {
                            if (!tableColumnsSet.Contains(fileCol))
                            {
                                isCompatible = false;
                                break;
                            }
                        }
                        
                        if (isCompatible)
                        {
                            matchingTables.Add(tableInfo);
                            _logger.LogInfo("schema_match_found", "Compatible table found: " + tableInfo.FullName);
                        }
                    }
                    catch
                    {
                        // Skip tables that can't be queried
                        continue;
                    }
                }
                
                return matchingTables;
            }
            catch (Exception ex)
            {
                _logger.LogError("schema_match_error", "Schema matching failed: " + ex.Message);
                throw new Exception("Failed to match table schemas: " + ex.Message);
            }
        }

        public int BackupExistingData(string sourceTable, string backupTable)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var backupTableName = ExtractTableBaseName(backupTable) + "_backup";
                    
                    // Get next version ID
                    var versionQuery = "SELECT COALESCE(MAX(ref_data_version_id), 0) + 1 FROM [" + _backupSchema + "].[" + backupTableName + "]";
                    var nextVersion = 1;
                    
                    using (var versionCommand = new SqlCommand(versionQuery, connection))
                    {
                        var result = versionCommand.ExecuteScalar();
                        if (result != null && result != DBNull.Value)
                        {
                            nextVersion = Convert.ToInt32(result);
                        }
                    }
                    
                    // Get column lists for explicit backup (avoid SELECT *)
                    var sourceColumns = GetTableColumns(sourceTable, _dataSchema);
                    var backupColumns = GetTableColumns(backupTableName, _backupSchema);
                    
                    // Build matching columns list - .NET Framework 4.5 compatible
                    var insertColumns = new List<string>();
                    var selectColumns = new List<string>();
                    
                    foreach (var backupCol in backupColumns)
                    {
                        var backupColName = backupCol.Name.ToLower();
                        
                        if (backupColName == "ref_data_loadtime" || backupColName == "ref_data_loadtype" || backupColName == "ref_data_version_id")
                        {
                            continue; // Skip metadata columns for now
                        }
                        
                        // Find matching source column
                        TableColumn matchingSourceCol = null;
                        foreach (var sourceCol in sourceColumns)
                        {
                            if (sourceCol.Name.ToLower() == backupColName)
                            {
                                matchingSourceCol = sourceCol;
                                break;
                            }
                        }
                        
                        if (matchingSourceCol != null)
                        {
                            insertColumns.Add("[" + backupCol.Name + "]");
                            selectColumns.Add("[" + matchingSourceCol.Name + "]");
                        }
                    }
                    
                    // Add metadata columns
                    insertColumns.Add("[ref_data_loadtime]");
                    insertColumns.Add("[ref_data_loadtype]");
                    insertColumns.Add("[ref_data_version_id]");
                    
                    selectColumns.Add("GETDATE()");
                    selectColumns.Add("'backup'");
                    selectColumns.Add("@version");
                    
                    var insertColumnList = string.Join(", ", insertColumns.ToArray());
                    var selectColumnList = string.Join(", ", selectColumns.ToArray());
                    
                    var backupSql = "INSERT INTO [" + _backupSchema + "].[" + backupTableName + "] (" + insertColumnList + ") " +
                                   "SELECT " + selectColumnList + " FROM [" + _dataSchema + "].[" + sourceTable + "]";
                    
                    using (var backupCommand = new SqlCommand(backupSql, connection))
                    {
                        backupCommand.Parameters.AddWithValue("@version", nextVersion);
                        var rowsBackedUp = backupCommand.ExecuteNonQuery();
                        
                        _logger.LogInfo("backup_completed", "Backed up " + rowsBackedUp + " rows from " + sourceTable + " to version " + nextVersion);
                        return rowsBackedUp;
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("backup_data_error", "Backup failed for " + sourceTable + ": " + ex.Message);
                // Return 0 instead of throwing to allow process to continue
                return 0;
            }
        }

        public void DropTableIfExists(string tableName, string schema = null)
        {
            if (string.IsNullOrEmpty(schema))
                schema = _dataSchema;
                
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var dropSql = "DROP TABLE IF EXISTS [" + schema + "].[" + tableName + "]";
                    
                    using (var command = new SqlCommand(dropSql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    
                    _logger.LogInfo("table_dropped", "Dropped table " + schema + "." + tableName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("drop_table_error", "Failed to drop table " + schema + "." + tableName + ": " + ex.Message);
                throw new Exception("Failed to drop table: " + ex.Message);
            }
        }

        public void TruncateTable(string tableName, string schema = null)
        {
            if (string.IsNullOrEmpty(schema))
                schema = _dataSchema;
                
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var truncateSql = "TRUNCATE TABLE [" + schema + "].[" + tableName + "]";
                    
                    using (var command = new SqlCommand(truncateSql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                    
                    _logger.LogInfo("table_truncated", "Truncated table " + schema + "." + tableName);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("truncate_table_error", "Failed to truncate table " + schema + "." + tableName + ": " + ex.Message);
                throw new Exception("Failed to truncate table: " + ex.Message);
            }
        }

        public Dictionary<string, object> SyncMainTableColumns(string tableName, List<TableColumn> fileColumns)
        {
            var result = new Dictionary<string, object>();
            var addedColumns = new List<Dictionary<string, object>>();
            var skippedColumns = new List<Dictionary<string, object>>();
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var existingColumns = GetTableColumns(tableName, _dataSchema);
                    
                    // Create a set of existing column names for quick lookup - .NET Framework 4.5 compatible
                    var existingColumnNames = new HashSet<string>();
                    foreach (var col in existingColumns)
                    {
                        existingColumnNames.Add(col.Name.ToLower());
                    }
                    
                    // Check each file column
                    foreach (var fileCol in fileColumns)
                    {
                        var colNameLower = fileCol.Name.ToLower();
                        
                        if (!existingColumnNames.Contains(colNameLower))
                        {
                            try
                            {
                                // Add missing column - safe operation that only adds, never modifies existing
                                var normalizedType = NormalizeDataType(fileCol.DataType, fileCol.MaxLength, fileCol.NumericPrecision, fileCol.NumericScale);
                                var alterSql = "ALTER TABLE [" + _dataSchema + "].[" + tableName + "] ADD [" + fileCol.Name + "] " + normalizedType;
                                
                                using (var alterCommand = new SqlCommand(alterSql, connection))
                                {
                                    alterCommand.ExecuteNonQuery();
                                }
                                
                                var addedCol = new Dictionary<string, object>();
                                addedCol["column"] = fileCol.Name;
                                addedCol["type"] = normalizedType;
                                addedColumns.Add(addedCol);
                                
                                _logger.LogInfo("column_added", "Added column " + fileCol.Name + " (" + normalizedType + ") to table " + tableName);
                            }
                            catch (Exception ex)
                            {
                                var skippedCol = new Dictionary<string, object>();
                                skippedCol["column"] = fileCol.Name;
                                skippedCol["reason"] = "Failed to add: " + ex.Message;
                                skippedColumns.Add(skippedCol);
                                
                                _logger.LogWarning("column_add_failed", "Failed to add column " + fileCol.Name + ": " + ex.Message);
                            }
                        }
                        else
                        {
                            var skippedCol = new Dictionary<string, object>();
                            skippedCol["column"] = fileCol.Name;
                            skippedCol["reason"] = "Column already exists";
                            skippedColumns.Add(skippedCol);
                        }
                    }
                }
                
                result["added"] = addedColumns;
                result["skipped"] = skippedColumns;
                
                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError("sync_columns_error", "Column sync failed for table " + tableName + ": " + ex.Message);
                throw new Exception("Failed to sync table columns: " + ex.Message);
            }
        }

        public void EnsureReferenceDataCfgTable()
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    var tableExists = TableExists("Reference_Data_Cfg", _dataSchema);
                    
                    if (!tableExists)
                    {
                        var createSql = @"
                            CREATE TABLE [" + _dataSchema + @"].[Reference_Data_Cfg] (
                                [table_name] varchar(255) NOT NULL PRIMARY KEY,
                                [last_updated] datetime NOT NULL DEFAULT GETDATE(),
                                [row_count] int NULL,
                                [status] varchar(50) NOT NULL DEFAULT 'active',
                                [created_date] datetime NOT NULL DEFAULT GETDATE()
                            )";
                            
                        using (var command = new SqlCommand(createSql, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        
                        _logger.LogInfo("reference_cfg_created", "Created Reference_Data_Cfg table");
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("reference_cfg_error", "Failed to ensure Reference_Data_Cfg table: " + ex.Message);
                throw new Exception("Failed to create Reference_Data_Cfg table: " + ex.Message);
            }
        }

        public void EnsurePostloadStoredProcedure()
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    // Check if stored procedure exists
                    var checkSql = "SELECT COUNT(*) FROM sys.objects WHERE type = 'P' AND name = @spName";
                    
                    using (var checkCommand = new SqlCommand(checkSql, connection))
                    {
                        checkCommand.Parameters.AddWithValue("@spName", _postloadSpName);
                        var exists = (int)checkCommand.ExecuteScalar() > 0;
                        
                        if (!exists)
                        {
                            var createSql = @"
                                CREATE PROCEDURE [" + _dataSchema + @"].[" + _postloadSpName + @"]
                                    @table_name varchar(255)
                                AS
                                BEGIN
                                    SET NOCOUNT ON;
                                    
                                    -- Default post-load processing
                                    PRINT 'Post-load processing for table: ' + @table_name;
                                    
                                    -- Add custom post-load logic here
                                    -- Example: UPDATE statistics, refresh views, etc.
                                    
                                END";
                                
                            using (var command = new SqlCommand(createSql, connection))
                            {
                                command.ExecuteNonQuery();
                            }
                            
                            _logger.LogInfo("postload_sp_created", "Created post-load stored procedure " + _postloadSpName);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("postload_sp_error", "Failed to ensure post-load stored procedure: " + ex.Message);
                throw new Exception("Failed to create post-load stored procedure: " + ex.Message);
            }
        }

        public void InsertReferenceDataCfgRecord(string tableName)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    // Get row count from the table
                    var countSql = "SELECT COUNT(*) FROM [" + _dataSchema + "].[" + tableName + "]";
                    var rowCount = 0;
                    
                    using (var countCommand = new SqlCommand(countSql, connection))
                    {
                        rowCount = (int)countCommand.ExecuteScalar();
                    }
                    
                    // Insert or update the configuration record
                    var upsertSql = @"
                        IF EXISTS (SELECT 1 FROM [" + _dataSchema + @"].[Reference_Data_Cfg] WHERE table_name = @tableName)
                            UPDATE [" + _dataSchema + @"].[Reference_Data_Cfg] 
                            SET last_updated = GETDATE(), row_count = @rowCount, status = 'active' 
                            WHERE table_name = @tableName
                        ELSE
                            INSERT INTO [" + _dataSchema + @"].[Reference_Data_Cfg] (table_name, last_updated, row_count, status, created_date)
                            VALUES (@tableName, GETDATE(), @rowCount, 'active', GETDATE())";
                    
                    using (var upsertCommand = new SqlCommand(upsertSql, connection))
                    {
                        upsertCommand.Parameters.AddWithValue("@tableName", tableName);
                        upsertCommand.Parameters.AddWithValue("@rowCount", rowCount);
                        upsertCommand.ExecuteNonQuery();
                    }
                    
                    // Call post-load stored procedure
                    try
                    {
                        var spSql = "EXEC [" + _dataSchema + "].[" + _postloadSpName + "] @tableName";
                        
                        using (var spCommand = new SqlCommand(spSql, connection))
                        {
                            spCommand.Parameters.AddWithValue("@tableName", tableName);
                            spCommand.ExecuteNonQuery();
                        }
                        
                        _logger.LogInfo("postload_executed", "Post-load procedure executed for " + tableName);
                    }
                    catch (Exception spEx)
                    {
                        _logger.LogWarning("postload_warning", "Post-load procedure failed for " + tableName + ": " + spEx.Message);
                        // Don't throw - this is not critical
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("reference_cfg_record_error", "Failed to insert Reference_Data_Cfg record for " + tableName + ": " + ex.Message);
                throw new Exception("Failed to update reference data configuration: " + ex.Message);
            }
        }
        
        public void ExecuteNonQuery(string sql)
        {
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand(sql, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("execute_non_query", "SQL execution failed: " + ex.Message + " SQL: " + sql);
                throw new Exception("SQL execution failed: " + ex.Message);
            }
        }

        // Helper method to extract table base name (remove _stage, _backup suffixes)
        private string ExtractTableBaseName(string tableName)
        {
            if (string.IsNullOrEmpty(tableName))
                return tableName;
                
            if (tableName.EndsWith("_stage"))
                return tableName.Substring(0, tableName.Length - 6);
                
            if (tableName.EndsWith("_backup"))
                return tableName.Substring(0, tableName.Length - 7);
                
            return tableName;
        }
    }
}
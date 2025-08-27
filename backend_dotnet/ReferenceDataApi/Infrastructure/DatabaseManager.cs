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
        private readonly string _database;
        private readonly string _staffDatabase;
        private readonly ILogger _logger;

        public DatabaseManager(IConfiguration configuration, ILogger logger)
        {
            var rawConnectionString = configuration.GetConnectionString("DefaultConnection");
            _connectionString = ExpandEnvironmentVariables(rawConnectionString);
            
            // Core database settings - with environment variable expansion
            _dataSchema = ExpandEnvironmentVariables(configuration["DatabaseSettings:DataSchema"]) ?? "ref";
            _backupSchema = ExpandEnvironmentVariables(configuration["DatabaseSettings:BackupSchema"]) ?? "bkp";  
            _postloadSpName = ExpandEnvironmentVariables(configuration["DatabaseSettings:PostloadStoredProcedure"]) ?? "sp_ref_postload";
            _database = ExpandEnvironmentVariables(configuration["DatabaseSettings:Database"]) ?? ExtractDatabaseNameFromConnectionString(_connectionString);
            _staffDatabase = ExpandEnvironmentVariables(configuration["DatabaseSettings:StaffDatabase"]) ?? "StaffDatabase";
            
            _logger = logger;
            
            // Log configuration info for debugging
            _logger.LogInfo("database_config", string.Format("Database: {0}, DataSchema: {1}, BackupSchema: {2}, Server: {3}", 
                _database, _dataSchema, _backupSchema, ExtractServerFromConnectionString(_connectionString)));
        }

        private string ExpandEnvironmentVariables(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            try
            {
                // Expand environment variables in format ${VAR_NAME:default_value}
                var result = System.Text.RegularExpressions.Regex.Replace(input, @"\$\{([^}:]+)(?::([^}]*))?\}", match =>
                {
                    var varName = match.Groups[1].Value;
                    var defaultValue = match.Groups.Count > 2 ? match.Groups[2].Value : "";
                    var envValue = Environment.GetEnvironmentVariable(varName);
                    return envValue ?? defaultValue;
                });

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError("env_var_expansion_error", "Error expanding environment variables: " + ex.Message);
                return input; // Return original string if expansion fails
            }
        }

        private string ExtractServerFromConnectionString(string connectionString)
        {
            try
            {
                var parts = connectionString.Split(';');
                foreach (var part in parts)
                {
                    var keyValue = part.Split('=');
                    if (keyValue.Length == 2 && keyValue[0].Trim().ToLower() == "server")
                    {
                        return keyValue[1].Trim();
                    }
                }
                return "Unknown";
            }
            catch
            {
                return "Unknown";
            }
        }

        private string ExtractDatabaseNameFromConnectionString(string connectionString)
        {
            try
            {
                // Extract database name from connection string
                var parts = connectionString.Split(';');
                foreach (var part in parts)
                {
                    var keyValue = part.Split('=');
                    if (keyValue.Length == 2 && keyValue[0].Trim().ToLower() == "database")
                    {
                        return keyValue[1].Trim();
                    }
                }
                
                // If not found, use a safe default
                _logger.LogWarning("database_name_extraction", "Could not extract database name from connection string, using default 'ReferenceDataDB'");
                return "ReferenceDataDB";
            }
            catch (Exception ex)
            {
                _logger.LogError("database_name_extraction_error", "Error extracting database name: " + ex.Message);
                return "ReferenceDataDB";
            }
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
                    
                    // Check if table exists in StaffDB.dbo schema
                    var checkSql = @"
                        SELECT COUNT(*) 
                        FROM INFORMATION_SCHEMA.TABLES 
                        WHERE TABLE_CATALOG = @database AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'Reference_Data_Cfg'";
                        
                    bool tableExists = false;
                    using (var checkCommand = new SqlCommand(checkSql, connection))
                    {
                        checkCommand.Parameters.AddWithValue("@database", _staffDatabase);
                        var count = (int)checkCommand.ExecuteScalar();
                        tableExists = count > 0;
                    }
                    
                    if (!tableExists)
                    {
                        var createSql = @"
                            CREATE TABLE [" + _staffDatabase + @"].[dbo].[Reference_Data_Cfg] (
                                [sp_name] varchar(255) NULL,
                                [ref_name] varchar(255) NOT NULL,
                                [source_db] varchar(255) NULL,
                                [source_schema] varchar(255) NULL,
                                [source_table] varchar(255) NULL,
                                [is_enabled] bit NOT NULL DEFAULT 1,
                                [last_updated] datetime NOT NULL DEFAULT GETDATE(),
                                [created_date] datetime NOT NULL DEFAULT GETDATE(),
                                CONSTRAINT PK_Reference_Data_Cfg PRIMARY KEY ([ref_name])
                            )";
                            
                        using (var command = new SqlCommand(createSql, connection))
                        {
                            command.ExecuteNonQuery();
                        }
                        
                        _logger.LogInfo("reference_cfg_created", "Created Reference_Data_Cfg table in " + _staffDatabase + ".dbo");
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

        public List<Dictionary<string, object>> GetReferenceDataConfig()
        {
            var results = new List<Dictionary<string, object>>();

            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "SELECT [sp_name], [ref_name], [source_db], [source_schema], [source_table], [is_enabled] FROM [" + _staffDatabase + "].[dbo].[Reference_Data_Cfg] ORDER BY [ref_name]";
                        
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var record = new Dictionary<string, object>();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    record[reader.GetName(i)] = reader.IsDBNull(i) ? null : reader.GetValue(i);
                                }
                                results.Add(record);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("get_reference_data_config", "Failed to retrieve Reference_Data_Cfg records: " + ex.Message);
                throw new Exception("Failed to retrieve reference data configuration: " + ex.Message);
            }

            return results;
        }

        public List<Dictionary<string, object>> GetAllTablesWithSchemas()
        {
            var results = new List<Dictionary<string, object>>();

            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    using (var command = new SqlCommand())
                    {
                        command.Connection = connection;
                        command.CommandText = "SELECT TABLE_SCHEMA, TABLE_NAME FROM [" + _database + "].INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_SCHEMA, TABLE_NAME";
                        
                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var table = new Dictionary<string, object>
                                {
                                    {"schema", reader.GetString(0)},
                                    {"table", reader.GetString(1)},
                                    {"full_name", reader.GetString(0) + "." + reader.GetString(1)}
                                };
                                results.Add(table);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("get_all_tables_with_schemas", "Failed to retrieve all tables with schemas: " + ex.Message);
                throw new Exception("Failed to retrieve all tables: " + ex.Message);
            }

            return results;
        }

        public Dictionary<string, object> VerifyLoadType(string filename, string loadMode)
        {
            try
            {
                // Extract table name from filename (remove extension and special characters)
                var tableName = System.IO.Path.GetFileNameWithoutExtension(filename);
                tableName = System.Text.RegularExpressions.Regex.Replace(tableName, @"[^a-zA-Z0-9_]", "_");
                
                // Determine what the load type would be
                var requestedLoadType = loadMode.ToLower() == "full" ? "F" : "A";
                var currentLoadType = requestedLoadType; // Default to requested
                
                // Check if table exists and has existing load types
                var existingLoadTypes = new List<string>();
                var tableExists = TableExists(tableName, _dataSchema);
                
                if (tableExists)
                {
                    using (var connection = new SqlConnection(_connectionString))
                    {
                        connection.Open();
                        using (var command = new SqlCommand())
                        {
                            command.Connection = connection;
                            command.CommandText = "SELECT DISTINCT [ref_data_loadtype] FROM [" + _dataSchema + "].[" + tableName + "] WHERE [ref_data_loadtype] IS NOT NULL";
                            
                            try
                            {
                                using (var reader = command.ExecuteReader())
                                {
                                    while (reader.Read())
                                    {
                                        var loadType = reader.GetString(0);
                                        if (!string.IsNullOrEmpty(loadType))
                                        {
                                            existingLoadTypes.Add(loadType.Trim());
                                        }
                                    }
                                }
                            }
                            catch
                            {
                                // Table might not have ref_data_loadtype column yet
                            }
                        }
                    }
                }
                
                var hasMismatch = currentLoadType != requestedLoadType;
                
                var result = new Dictionary<string, object>
                {
                    {"table_name", tableName},
                    {"requested_load_mode", loadMode},
                    {"requested_load_type", requestedLoadType},
                    {"determined_load_type", currentLoadType},
                    {"has_mismatch", hasMismatch},
                    {"existing_load_types", existingLoadTypes},
                    {"table_exists", tableExists},
                    {"explanation", "Based on existing data, load type will be '" + currentLoadType + "' but you requested '" + requestedLoadType + "'"}
                };

                return result;
            }
            catch (Exception ex)
            {
                _logger.LogError("verify_load_type", "Failed to verify load type for " + filename + ": " + ex.Message);
                throw new Exception("Failed to verify load type: " + ex.Message);
            }
        }

        public List<BackupInfo> GetBackupTables(string tableFilter = null)
        {
            var backups = new List<BackupInfo>();
            
            try
            {
                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();
                    
                    // First get all backup tables
                    var sql = @"
                        SELECT 
                            b.TABLE_NAME as backup_table_name,
                            REPLACE(b.TABLE_NAME, '_backup', '') as base_name,
                            CASE WHEN m.TABLE_NAME IS NOT NULL THEN 1 ELSE 0 END as has_main,
                            CASE WHEN s.TABLE_NAME IS NOT NULL THEN 1 ELSE 0 END as has_stage
                        FROM [" + _database + @"].INFORMATION_SCHEMA.TABLES b
                        LEFT JOIN [" + _database + @"].INFORMATION_SCHEMA.TABLES m ON 
                            m.TABLE_SCHEMA = '" + _dataSchema + @"' AND 
                            m.TABLE_NAME = REPLACE(b.TABLE_NAME, '_backup', '')
                        LEFT JOIN [" + _database + @"].INFORMATION_SCHEMA.TABLES s ON 
                            s.TABLE_SCHEMA = '" + _dataSchema + @"' AND 
                            s.TABLE_NAME = REPLACE(b.TABLE_NAME, '_backup', '') + '_stage'
                        WHERE b.TABLE_SCHEMA = '" + _backupSchema + @"' 
                            AND b.TABLE_NAME LIKE '%_backup'
                            AND b.TABLE_TYPE = 'BASE TABLE'";

                    if (!string.IsNullOrEmpty(tableFilter))
                    {
                        sql += " AND REPLACE(b.TABLE_NAME, '_backup', '') LIKE @tableFilter";
                    }

                    sql += " ORDER BY b.TABLE_NAME";

                    using (var command = new SqlCommand(sql, connection))
                    {
                        if (!string.IsNullOrEmpty(tableFilter))
                        {
                            command.Parameters.AddWithValue("@tableFilter", "%" + tableFilter + "%");
                        }

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                var baseName = reader["base_name"] != null ? reader["base_name"].ToString() : "";
                                var backupTableName = reader["backup_table_name"] != null ? reader["backup_table_name"].ToString() : "";
                                
                                backups.Add(new BackupInfo
                                {
                                    Table = baseName,
                                    BackupTable = backupTableName,
                                    BaseName = baseName,
                                    HasMain = Convert.ToBoolean(reader["has_main"]),
                                    HasStage = Convert.ToBoolean(reader["has_stage"]),
                                    VersionCount = 0, // Will be populated separately
                                    LatestVersion = 0, // Will be populated separately 
                                    RowCount = 0, // Will be populated separately
                                    CreatedDate = DateTime.UtcNow,
                                    Status = "active",
                                    VersionId = 0 // Will be populated separately
                                });
                            }
                        }
                    }

                    // Now get version and row count data for each backup table separately
                    foreach (var backup in backups)
                    {
                        try
                        {
                            using (var connection2 = new SqlConnection(_connectionString))
                            {
                                connection2.Open();
                                
                                // Check if backup table exists and get metadata
                                var metaQuery = string.Format(@"
                                    IF EXISTS (SELECT 1 FROM [{0}].INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{1}' AND TABLE_NAME = '{2}')
                                    BEGIN
                                        SELECT 
                                            ISNULL(COUNT(DISTINCT ref_data_version_id), 0) as version_count,
                                            ISNULL(MAX(ref_data_version_id), 0) as latest_version,
                                            ISNULL(COUNT(*), 0) as row_count,
                                            ISNULL(MAX(ref_data_loadtime), GETDATE()) as latest_created_date
                                        FROM [{0}].[{1}].[{2}]
                                    END
                                    ELSE
                                    BEGIN
                                        SELECT 0 as version_count, 0 as latest_version, 0 as row_count, GETDATE() as latest_created_date
                                    END", _database, _backupSchema, backup.BackupTable);
                                
                                using (var metaCommand = new SqlCommand(metaQuery, connection2))
                                using (var metaReader = metaCommand.ExecuteReader())
                                {
                                    if (metaReader.Read())
                                    {
                                        backup.VersionCount = Convert.ToInt32(metaReader["version_count"]);
                                        backup.LatestVersion = Convert.ToInt32(metaReader["latest_version"]);
                                        backup.RowCount = Convert.ToInt32(metaReader["row_count"]);
                                        backup.CreatedDate = Convert.ToDateTime(metaReader["latest_created_date"]);
                                        backup.VersionId = backup.LatestVersion ?? 0;
                                    }
                                }
                            }
                        }
                        catch (Exception metaEx)
                        {
                            _logger.LogError("backup_metadata_error", "Failed to get metadata for " + backup.BackupTable + ": " + metaEx.Message);
                            // Keep default values on error
                        }
                    }
                }
                
                _logger.LogInfo("get_backup_tables", "Retrieved " + backups.Count + " backup table entries");
                return backups;
            }
            catch (Exception ex)
            {
                _logger.LogError("get_backup_tables_error", "Failed to get backup tables: " + ex.Message);
                return new List<BackupInfo>(); // Return empty list on error
            }
        }

        public Dictionary<string, object> RollbackTableToVersion(string tableName, int versionId)
        {
            try
            {
                var baseName = ExtractTableBaseName(tableName);
                var backupTableName = baseName + "_backup";
                var mainTableName = baseName;
                var stageTableName = baseName + "_stage";

                using (var connection = new SqlConnection(_connectionString))
                {
                    connection.Open();

                    // Step 1: Validate the backup version exists
                    var versionExistsQuery = "SELECT COUNT(*) FROM [" + _backupSchema + "].[" + backupTableName + "] WHERE ref_data_version_id = @versionId";
                    using (var cmd = new SqlCommand(versionExistsQuery, connection))
                    {
                        cmd.Parameters.AddWithValue("@versionId", versionId);
                        var versionCount = Convert.ToInt32(cmd.ExecuteScalar());
                        if (versionCount == 0)
                        {
                            throw new Exception("Backup version " + versionId + " does not exist for table " + tableName);
                        }
                    }

                    // Step 2: Create backup of current data before rollback
                    var currentDataExists = TableExists(mainTableName, _dataSchema);
                    int currentRowsBackedUp = 0;
                    if (currentDataExists)
                    {
                        currentRowsBackedUp = BackupExistingData(mainTableName, backupTableName);
                        _logger.LogInfo("rollback_backup_current", "Backed up " + currentRowsBackedUp + " current rows before rollback");
                    }

                    // Step 3: Get columns from main table to ensure proper data transfer
                    var mainTableColumns = GetTableColumns(mainTableName, _dataSchema);
                    var columnNames = mainTableColumns.Where(c => 
                        !c.Name.ToLower().Equals("ref_data_loadtime") && 
                        !c.Name.ToLower().Equals("ref_data_loadtype") && 
                        !c.Name.ToLower().Equals("ref_data_version_id")
                    ).Select(c => "[" + c.Name + "]").ToList();

                    // Step 4: Clear current main table
                    TruncateTable(mainTableName, _dataSchema);

                    // Step 5: Restore data from backup version
                    var restoreColumns = string.Join(", ", columnNames);
                    var restoreSql = "INSERT INTO [" + _dataSchema + "].[" + mainTableName + "] (" + restoreColumns + 
                                   ", [ref_data_loadtime], [ref_data_loadtype]) " +
                                   "SELECT " + restoreColumns + ", GETDATE(), 'rollback' " +
                                   "FROM [" + _backupSchema + "].[" + backupTableName + "] " +
                                   "WHERE ref_data_version_id = @versionId";

                    int restoredRows = 0;
                    using (var restoreCmd = new SqlCommand(restoreSql, connection))
                    {
                        restoreCmd.Parameters.AddWithValue("@versionId", versionId);
                        restoredRows = restoreCmd.ExecuteNonQuery();
                    }

                    // Step 6: Clear stage table if it exists
                    int stageRowsCleared = 0;
                    if (TableExists(stageTableName, _dataSchema))
                    {
                        var stageCountQuery = "SELECT COUNT(*) FROM [" + _dataSchema + "].[" + stageTableName + "]";
                        using (var countCmd = new SqlCommand(stageCountQuery, connection))
                        {
                            stageRowsCleared = Convert.ToInt32(countCmd.ExecuteScalar());
                        }
                        TruncateTable(stageTableName, _dataSchema);
                    }

                    // Step 7: Update Reference_Data_Cfg table
                    var updateCfgSql = "UPDATE [" + _dataSchema + "].[Reference_Data_Cfg] " +
                                     "SET last_updated = GETDATE(), row_count = @rowCount, status = 'rolled_back' " +
                                     "WHERE table_name = @tableName";
                    using (var updateCmd = new SqlCommand(updateCfgSql, connection))
                    {
                        updateCmd.Parameters.AddWithValue("@rowCount", restoredRows);
                        updateCmd.Parameters.AddWithValue("@tableName", mainTableName);
                        updateCmd.ExecuteNonQuery();
                    }

                    _logger.LogInfo("rollback_completed", "Successfully rolled back table " + tableName + " to version " + versionId);

                    // Return detailed results
                    return new Dictionary<string, object>
                    {
                        {"success", true},
                        {"table", tableName},
                        {"version_id", versionId},
                        {"main_rows_restored", restoredRows},
                        {"stage_rows_cleared", stageRowsCleared},
                        {"current_rows_backed_up", currentRowsBackedUp},
                        {"timestamp", DateTime.UtcNow}
                    };
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("rollback_error", "Failed to rollback table " + tableName + " to version " + versionId + ": " + ex.Message);
                throw new Exception("Rollback failed: " + ex.Message);
            }
        }
    }
}
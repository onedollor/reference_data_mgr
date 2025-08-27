using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using ReferenceDataApi.Infrastructure;

namespace ReferenceDataApi.Services
{
    public class DataIngestion : IDataIngestion
    {
        private readonly IDatabaseManager _databaseManager;
        private readonly IFileHandler _fileHandler;
        private readonly ILogger _logger;
        private readonly IProgressTracker _progressTracker;

        public DataIngestion(
            IDatabaseManager databaseManager,
            IFileHandler fileHandler,
            ILogger logger,
            IProgressTracker progressTracker)
        {
            _databaseManager = databaseManager;
            _fileHandler = fileHandler;
            _logger = logger;
            _progressTracker = progressTracker;
        }

        public void ProcessFileBackground(string filePath, string tableName, string targetSchema,
            Dictionary<string, string> formatConfig, bool configReferenceData, string progressKey)
        {
            try
            {
                _progressTracker.UpdateProgress(progressKey, "starting", 5, "Starting data ingestion process");
                _logger.LogInfo("ingestion_start", "Starting background ingestion for " + tableName);

                // Step 1: Parse the CSV file using the format configuration
                _progressTracker.UpdateProgress(progressKey, "parsing", 20, "Parsing CSV file");
                
                var csvData = ParseCsvFile(filePath, formatConfig);
                if (csvData == null || csvData.Count == 0)
                {
                    throw new Exception("No data found in CSV file");
                }

                var headers = csvData[0];
                var dataRows = csvData.Skip(1).ToList();

                _logger.LogInfo("csv_parsed", "Parsed CSV with " + headers.Count + " columns and " + dataRows.Count + " data rows");
                
                // Step 2: Create/validate database table
                _progressTracker.UpdateProgress(progressKey, "validating", 40, "Creating database table");
                
                var fullTableName = targetSchema + "." + tableName;
                CreateTableFromHeaders(fullTableName, headers);
                _logger.LogInfo("table_created", "Created table " + fullTableName);
                
                // Step 2.5: Handle backup if reference data configuration is enabled
                if (configReferenceData)
                {
                    _progressTracker.UpdateProgress(progressKey, "backing_up", 55, "Creating backup table and backing up existing data");
                    
                    // Create backup table if it doesn't exist
                    var tableColumns = _databaseManager.GetTableColumns(tableName, targetSchema);
                    _databaseManager.CreateBackupTable(tableName, tableColumns);
                    _logger.LogInfo("backup_table_created", "Backup table created for " + tableName);
                    
                    // Backup existing data if table has data
                    if (_databaseManager.TableExists(tableName, targetSchema))
                    {
                        var backedUpRows = _databaseManager.BackupExistingData(fullTableName, tableName + "_backup");
                        _logger.LogInfo("data_backed_up", "Backed up " + backedUpRows + " rows from " + fullTableName);
                    }
                }
                
                // Step 3: Import data in batches
                _progressTracker.UpdateProgress(progressKey, "importing", 70, "Importing data to database");
                
                ImportDataToTable(fullTableName, headers, dataRows);
                _logger.LogInfo("data_imported", "Imported " + dataRows.Count + " rows to " + fullTableName);
                
                // Step 4: Finalize
                _progressTracker.UpdateProgress(progressKey, "finalizing", 90, "Finalizing import process");
                
                // Clean up temp file
                if (System.IO.File.Exists(filePath))
                {
                    System.IO.File.Delete(filePath);
                }

                _progressTracker.MarkDone(progressKey);
                _logger.LogInfo("ingestion_complete", "Background ingestion completed for " + tableName);
            }
            catch (Exception ex)
            {
                var errorMessage = "Data ingestion failed: " + ex.Message;
                _progressTracker.MarkError(progressKey, errorMessage);
                _logger.LogError("ingestion_error", errorMessage);
                
                // Clean up temp file on error
                if (System.IO.File.Exists(filePath))
                {
                    try { System.IO.File.Delete(filePath); } catch { }
                }
            }
        }

        private List<List<string>> ParseCsvFile(string filePath, Dictionary<string, string> formatConfig)
        {
            var delimiter = formatConfig.ContainsKey("delimiter") ? formatConfig["delimiter"] : ",";
            var result = new List<List<string>>();

            try
            {
                using (var reader = new StreamReader(filePath))
                using (var csv = new CsvReader(reader))
                {
                    // Configure for version 12.3.2 API
                    csv.Configuration.Delimiter = delimiter;
                    csv.Configuration.HasHeaderRecord = false;
                    csv.Configuration.BadDataFound = null; // Don't throw on bad data
                    csv.Configuration.MissingFieldFound = null; // Don't throw on missing fields
                    csv.Configuration.HeaderValidated = null; // Don't validate headers
                    
                    int lineNumber = 0;
                    while (csv.Read())
                    {
                        lineNumber++;
                        try
                        {
                            var fields = new List<string>();
                            
                            // Use the Context.Record for version 12.3.2
                            var fieldCount = csv.Context.Record.Length;
                            for (int i = 0; i < fieldCount; i++)
                            {
                                fields.Add(csv.GetField(i) ?? "");
                            }
                            
                            // Only add non-empty records
                            if (fields.Any(field => !string.IsNullOrWhiteSpace(field)))
                            {
                                result.Add(fields);
                            }
                        }
                        catch (Exception ex)
                        {
                            _logger.LogWarning("csv_parse_line_error", "Error parsing line " + lineNumber + ": " + ex.Message);
                            // Continue with next line - CsvHelper handles malformed CSV better
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("csv_parse_error", "Error parsing CSV file: " + ex.Message);
                // Fallback to empty result
                return new List<List<string>>();
            }

            _logger.LogInfo("csv_parse_complete", "Successfully parsed CSV with " + result.Count + " records using RFC 4180 compliant CsvHelper parser");
            return result;
        }

        private void CreateTableFromHeaders(string fullTableName, List<string> headers)
        {
            var tableNameParts = fullTableName.Split('.');
            var schemaName = tableNameParts[0];
            var tableName = tableNameParts[1];
            
            // Check if table already exists
            var tableExistsSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableName + "' AND TABLE_SCHEMA = '" + schemaName + "'";
            _logger.LogInfo("table_existence_check", "Checking if table exists with SQL: " + tableExistsSql);
            var result = _databaseManager.ExecuteScalar(tableExistsSql);
            _logger.LogInfo("table_existence_result", "Query result: " + (result ?? "null") + " for table " + fullTableName);
            var tableExists = Convert.ToInt32(result ?? 0) > 0;
            _logger.LogInfo("table_exists_decision", "Table " + fullTableName + " exists: " + tableExists);
            
            if (!tableExists)
            {
                // Create new table with data columns + metadata columns
                var columnDefinitions = headers.Select(header => "[" + header + "] VARCHAR(255)").ToList();
                columnDefinitions.Add("[ref_data_loadtime] DATETIME DEFAULT GETDATE()");
                columnDefinitions.Add("[ref_data_loadtype] VARCHAR(255)");
                
                var createTableSql = "CREATE TABLE " + fullTableName + " (" + string.Join(", ", columnDefinitions) + ")";
                _databaseManager.ExecuteNonQuery(createTableSql);
            }
            else
            {
                // Table exists - ensure metadata columns are present
                EnsureMetadataColumnsExist(fullTableName);
            }
        }
        
        private void EnsureMetadataColumnsExist(string fullTableName)
        {
            var tableNameParts = fullTableName.Split('.');
            var schemaName = tableNameParts[0];
            var tableName = tableNameParts[1];
            
            // Check if ref_data_loadtime column exists
            var loadtimeExistsSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' AND TABLE_SCHEMA = '" + schemaName + "' AND COLUMN_NAME = 'ref_data_loadtime'";
            var loadtimeExists = Convert.ToInt32(_databaseManager.ExecuteScalar(loadtimeExistsSql)) > 0;
            
            if (!loadtimeExists)
            {
                var addLoadtimeSql = "ALTER TABLE " + fullTableName + " ADD [ref_data_loadtime] DATETIME DEFAULT GETDATE()";
                _databaseManager.ExecuteNonQuery(addLoadtimeSql);
                _logger.LogInfo("metadata_column_added", "Added ref_data_loadtime column to " + fullTableName);
            }
            
            // Check if ref_data_loadtype column exists  
            var loadtypeExistsSql = "SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "' AND TABLE_SCHEMA = '" + schemaName + "' AND COLUMN_NAME = 'ref_data_loadtype'";
            var loadtypeExists = Convert.ToInt32(_databaseManager.ExecuteScalar(loadtypeExistsSql)) > 0;
            
            if (!loadtypeExists)
            {
                var addLoadtypeSql = "ALTER TABLE " + fullTableName + " ADD [ref_data_loadtype] VARCHAR(255)";
                _databaseManager.ExecuteNonQuery(addLoadtypeSql);
                _logger.LogInfo("metadata_column_added", "Added ref_data_loadtype column to " + fullTableName);
            }
        }

        private void ImportDataToTable(string fullTableName, List<string> headers, List<List<string>> dataRows)
        {
            var expectedColumnCount = headers.Count;
            var adjustedRows = 0;

            foreach (var row in dataRows)
            {
                var adjustedRow = new List<string>(row);
                
                // Handle column count mismatches by adjusting the row
                if (adjustedRow.Count != expectedColumnCount)
                {
                    if (adjustedRow.Count > expectedColumnCount)
                    {
                        // Too many columns - truncate extra ones
                        adjustedRow = adjustedRow.Take(expectedColumnCount).ToList();
                        _logger.LogWarning("column_truncated", 
                            "Truncated row from " + row.Count + " to " + expectedColumnCount + " columns: " + 
                            string.Join(", ", row.Take(5)) + (row.Count > 5 ? "..." : ""));
                    }
                    else
                    {
                        // Too few columns - pad with empty strings
                        while (adjustedRow.Count < expectedColumnCount)
                        {
                            adjustedRow.Add("");
                        }
                        _logger.LogWarning("column_padded", 
                            "Padded row from " + row.Count + " to " + expectedColumnCount + " columns: " + 
                            string.Join(", ", row.Take(5)) + (row.Count > 5 ? "..." : ""));
                    }
                    adjustedRows++;
                }

                var values = adjustedRow.Select(v => "'" + v.Replace("'", "''") + "'").ToList(); // Escape single quotes
                
                // Add metadata values
                values.Add("GETDATE()"); // ref_data_loadtime (using SQL function, not quoted)
                values.Add("'F'"); // ref_data_loadtype ('F' for Full load, 'A' for Append)
                
                // Include metadata columns in the INSERT
                var allColumns = headers.Select(h => "[" + h + "]").ToList();
                allColumns.Add("[ref_data_loadtime]");
                allColumns.Add("[ref_data_loadtype]");
                
                var headerColumns = string.Join(", ", allColumns);
                var insertSql = "INSERT INTO " + fullTableName + " (" + headerColumns + ") VALUES (" + string.Join(", ", values) + ")";
                
                try
                {
                    _databaseManager.ExecuteNonQuery(insertSql);
                }
                catch (Exception ex)
                {
                    _logger.LogError("insert_error", "Failed to insert row: " + ex.Message);
                    // Continue with other rows
                }
            }

            if (adjustedRows > 0)
            {
                _logger.LogInfo("rows_adjusted", "Adjusted " + adjustedRows + " rows for column count mismatch (padded missing or truncated extra columns)");
            }
        }
    }
}
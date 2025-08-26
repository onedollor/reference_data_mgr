using System;
using System.Collections.Generic;
using System.Linq;
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

            using (var reader = new System.IO.StreamReader(filePath))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    // Simple CSV parsing - in production would use more robust parser
                    var fields = line.Split(delimiter.ToCharArray()).Select(f => f.Trim('"')).ToList();
                    result.Add(fields);
                }
            }

            return result;
        }

        private void CreateTableFromHeaders(string fullTableName, List<string> headers)
        {
            // Create table with all VARCHAR columns for simplicity
            var columnDefinitions = headers.Select(header => "[" + header + "] VARCHAR(255)").ToList();
            var tableNameParts = fullTableName.Split('.');
            var createTableSql = "IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '" + tableNameParts[1] + "' AND TABLE_SCHEMA = '" + tableNameParts[0] + "') " +
                               "CREATE TABLE " + fullTableName + " (" + string.Join(", ", columnDefinitions) + ")";
            
            _databaseManager.ExecuteNonQuery(createTableSql);
        }

        private void ImportDataToTable(string fullTableName, List<string> headers, List<List<string>> dataRows)
        {
            foreach (var row in dataRows)
            {
                var values = row.Select(v => "'" + v.Replace("'", "''") + "'").ToList(); // Escape single quotes
                var headerColumns = string.Join(", ", headers.Select(h => "[" + h + "]"));
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
        }
    }
}
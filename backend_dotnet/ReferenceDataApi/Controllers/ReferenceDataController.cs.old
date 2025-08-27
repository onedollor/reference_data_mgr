using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Web;
using System.Web.Http;
using System.Configuration;
using System.Net.Http;
using System.Net;
using ReferenceDataApi.Infrastructure;
using ReferenceDataApi.Models;
using ReferenceDataApi.Services;

namespace ReferenceDataApi.Controllers
{
    public class ReferenceDataController : ApiController
    {
        private readonly IDatabaseManager _databaseManager;
        private readonly IFileHandler _fileHandler;
        private readonly ILogger _logger;
        private readonly IProgressTracker _progressTracker;
        private readonly ICsvDetector _csvDetector;
        private readonly IDataIngestion _dataIngestion;
        public ReferenceDataController()
        {
            _logger = new FileLogger(); // Simple implementation
            _databaseManager = new DatabaseManager(_logger);
            _fileHandler = new FileHandler(_logger);
            _progressTracker = new ProgressTracker();
            _csvDetector = new CsvDetector(_logger);
            _dataIngestion = new DataIngestion(_databaseManager, _logger, _progressTracker);
        }

        [HttpGet]
        [Route("")]
        public ApiResponse Root()
        {
            return new ApiResponse 
            { 
                Message = "Reference Data Auto Ingest System - .NET Framework 4.0 Backend",
                Version = "1.0.0",
                Timestamp = DateTime.UtcNow
            };
        }

        [HttpGet]
        [Route("health/database")]
        public HealthResponse DatabaseHealth()
        {
            try
            {
                var clientIp = GetClientIpAddress();
                _logger.LogInfo("database_health_check", "Health check request from " + clientIp);
                
                var isHealthy = _databaseManager.TestConnection();
                
                var response = new HealthResponse
                {
                    Status = isHealthy ? "healthy" : "unhealthy",
                    Database = isHealthy ? "connected" : "disconnected",
                    Timestamp = DateTime.UtcNow,
                    Message = isHealthy ? "Database connection successful" : "Database connection failed"
                };

                if (isHealthy)
                {
                    return Ok(response);
                }
                else
                {
                    return BadRequest(response);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("database_health_error", "Database health check failed: " + ex.Message);
                
                return BadRequest(new HealthResponse
                {
                    Status = "unhealthy", 
                    Database = "error",
                    Timestamp = DateTime.UtcNow,
                    Message = ex.Message
                });
            }
        }

        [HttpGet]
        [Route("config")]
        public HttpResponseMessage GetConfig()
        {
            try
            {
                var config = new ConfigResponse
                {
                    max_upload_size = int.Parse(ExpandEnvironmentVariables(_configuration["FileSettings:MaxUploadSize"]) ?? "20971520"), // 20MB default
                    supported_formats = new List<string> { "csv" },
                    default_delimiters = new DefaultDelimiters
                    {
                        header_delimiter = "|",
                        column_delimiter = "|",
                        row_delimiter = "|\"\"\\r\\n",
                        text_qualifier = "\""
                    },
                    delimiter_options = new DelimiterOptions
                    {
                        header_delimiter = new List<string> { ",", ";", "|", "\t" },
                        column_delimiter = new List<string> { ",", ";", "|", "\t" },
                        row_delimiter = new List<string> { "\r", "\n", "\r\n", "|\"\"\\r\\n" },
                        text_qualifier = new List<string> { "\"", "'", "\"\"" }
                    },
                    DatabaseSettings = new DatabaseSettings
                    {
                        DataSchema = ExpandEnvironmentVariables(_configuration["DatabaseSettings:DataSchema"]) ?? "ref",
                        BackupSchema = ExpandEnvironmentVariables(_configuration["DatabaseSettings:BackupSchema"]) ?? "bkp",
                        PostloadStoredProcedure = ExpandEnvironmentVariables(_configuration["DatabaseSettings:PostloadStoredProcedure"]) ?? "sp_ref_postload"
                    },
                    FileSettings = new FileSettings
                    {
                        UploadPath = ExpandEnvironmentVariables(_configuration["FileSettings:UploadPath"]) ?? "./uploads",
                        ArchivePath = ExpandEnvironmentVariables(_configuration["FileSettings:ArchivePath"]) ?? "./archive",
                        TempPath = ExpandEnvironmentVariables(_configuration["FileSettings:TempPath"]) ?? "./temp"
                    },
                    Timestamp = DateTime.UtcNow
                };

                return Ok(config);
            }
            catch (Exception ex)
            {
                _logger.LogError("config_error", "Failed to retrieve configuration: " + ex.Message);
                return BadRequest(new { error = "Failed to retrieve configuration: " + ex.Message });
            }
        }

        [HttpGet("/schemas")]
        public ActionResult<SchemasResponse> GetAvailableSchemas()
        {
            try
            {
                var schemas = _databaseManager.GetAvailableSchemas();
                var defaultSchema = _configuration["DatabaseSettings:DataSchema"];

                _logger.LogInfo("schemas_query", "Retrieved " + schemas.Count + " available schemas");

                return Ok(new SchemasResponse
                {
                    Schemas = schemas,
                    DefaultSchema = defaultSchema,
                    Count = schemas.Count
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve available schemas: " + ex.Message;
                _logger.LogError("schemas_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpGet("/features")]
        public ActionResult<FeaturesResponse> GetFeatureFlags()
        {
            var features = new FeaturesResponse
            {
                AsyncIngestion = true,
                ProgressTracking = true,
                BackupSupport = true,
                SchemaMatching = true,
                CsvDetection = true,
                RollbackSupport = true,
                LoggingEnabled = true
            };

            return Ok(features);
        }

        [HttpPost("/detect-format")]
        public ActionResult<FormatDetectionResponse> DetectCsvFormat(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest(new { error = "No file uploaded" });
            }

            try
            {
                // .NET Framework 4.5 compatible file reading
                var filePath = Path.GetTempFileName();
                
                using (var stream = new FileStream(filePath, FileMode.Create))
                {
                    file.CopyTo(stream);
                }

                var detectionResult = _csvDetector.DetectFormat(filePath);

                // Clean up temp file
                if (System.IO.File.Exists(filePath))
                {
                    System.IO.File.Delete(filePath);
                }

                return Ok(detectionResult);
            }
            catch (Exception ex)
            {
                _logger.LogError("format_detection_error", "CSV format detection failed: " + ex.Message);
                return BadRequest(new { error = "Format detection failed: " + ex.Message });
            }
        }

        [HttpGet("/tables")]
        public ActionResult<TablesResponse> GetAllTables()
        {
            try
            {
                var tables = _databaseManager.GetAllTables();
                _logger.LogInfo("tables_query", "Retrieved " + tables.Count + " tables");

                return Ok(new TablesResponse
                {
                    Tables = tables,
                    Count = tables.Count
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve tables: " + ex.Message;
                _logger.LogError("tables_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/tables/schema-match")]
        public ActionResult<SchemaMatchResponse> GetSchemaMatchedTables([FromBody] SchemaMatchRequest request)
        {
            if (request == null || request.Columns == null || request.Columns.Count == 0)
            {
                return BadRequest(new { error = "No columns provided for schema matching" });
            }

            try
            {
                var matchingTables = _databaseManager.GetSchemaMatchedTables(request.Columns);
                _logger.LogInfo("schema_match", "Found " + matchingTables.Count + " matching tables for " + request.Columns.Count + " columns");

                return Ok(new SchemaMatchResponse
                {
                    MatchingTables = matchingTables,
                    Count = matchingTables.Count,
                    InputColumns = request.Columns
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Schema matching failed: " + ex.Message;
                _logger.LogError("schema_match_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpGet("/progress/{key}")]
        public ActionResult<ProgressResponse> GetProgress(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                return BadRequest(new { error = "Progress key is required" });
            }

            try
            {
                var progress = _progressTracker.GetProgress(key);
                
                if (progress == null)
                {
                    return NotFound(new { error = "Progress key not found: " + key });
                }

                return Ok(progress);
            }
            catch (Exception ex)
            {
                _logger.LogError("progress_error", "Failed to get progress for key " + key + ": " + ex.Message);
                return BadRequest(new { error = "Failed to retrieve progress: " + ex.Message });
            }
        }

        [HttpPost("/progress/{key}/cancel")]
        public ActionResult<object> CancelProgress(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                return BadRequest(new { error = "Progress key is required" });
            }

            try
            {
                var cancelled = _progressTracker.CancelOperation(key);
                
                if (cancelled)
                {
                    _logger.LogInfo("operation_cancelled", "Cancelled operation with key: " + key);
                    return Ok(new { message = "Operation cancelled successfully", key = key });
                }
                else
                {
                    return BadRequest(new { error = "Failed to cancel operation or operation not found", key = key });
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("cancel_error", "Failed to cancel operation " + key + ": " + ex.Message);
                return BadRequest(new { error = "Failed to cancel operation: " + ex.Message });
            }
        }

        [HttpGet("/logs")]
        public ActionResult<LogsResponse> GetSystemLogs([FromQuery] int limit = 100, [FromQuery] string level = null)
        {
            try
            {
                var logs = ReadSystemLogsFromFile(limit, level);

                _logger.LogInfo("logs_query", "Retrieved " + logs.Count + " log entries from file");

                return Ok(new LogsResponse
                {
                    Logs = logs,
                    Count = logs.Count,
                    Limit = limit,
                    Level = level
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve logs: " + ex.Message;
                _logger.LogError("logs_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        private List<LogEntry> ReadSystemLogsFromFile(int limit, string level)
        {
            var logs = new List<LogEntry>();
            var logFilePath = Path.Combine("logs", "system.log");

            if (!System.IO.File.Exists(logFilePath))
            {
                return logs; // Return empty list if file doesn't exist
            }

            try
            {
                var lines = System.IO.File.ReadAllLines(logFilePath);
                
                // Read lines in reverse order to get most recent logs first
                for (int i = lines.Length - 1; i >= 0; i--)
                {
                    var line = lines[i];
                    if (string.IsNullOrWhiteSpace(line)) continue;

                    var logEntry = ParseLogLine(line);
                    if (logEntry != null)
                    {
                        // Filter by level if specified
                        if (string.IsNullOrEmpty(level) || logEntry.Level.Equals(level, StringComparison.OrdinalIgnoreCase))
                        {
                            logs.Add(logEntry);
                            
                            // Stop if we've reached the limit
                            if (logs.Count >= limit)
                            {
                                break;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                _logger.LogError("read_logs_error", "Failed to read log file: " + ex.Message);
            }

            return logs;
        }

        private LogEntry ParseLogLine(string line)
        {
            try
            {
                // Log format: "2025-08-25 21:58:45 [INFO] context: message"
                if (line.Length < 23) return null; // Minimum length check

                var timestampStr = line.Substring(0, 19); // "2025-08-25 21:58:45"
                DateTime timestamp;
                if (!DateTime.TryParseExact(timestampStr, "yyyy-MM-dd HH:mm:ss", null, DateTimeStyles.None, out timestamp))
                {
                    return null;
                }

                var levelStart = line.IndexOf('[');
                var levelEnd = line.IndexOf(']');
                if (levelStart == -1 || levelEnd == -1 || levelEnd <= levelStart) return null;

                var levelStr = line.Substring(levelStart + 1, levelEnd - levelStart - 1);

                var contextStart = levelEnd + 2;
                var contextEnd = line.IndexOf(':', contextStart);
                if (contextEnd == -1) return null;

                var contextStr = line.Substring(contextStart, contextEnd - contextStart);
                var messageStr = line.Substring(contextEnd + 2); // Skip ": "

                return new LogEntry
                {
                    Timestamp = timestamp,
                    Level = levelStr,
                    Context = contextStr,
                    Message = messageStr
                };
            }
            catch
            {
                return null; // Return null for unparseable lines
            }
        }

        [HttpGet("/backups")]
        public ActionResult<BackupsResponse> GetBackups([FromQuery] string table = null)
        {
            try
            {
                _logger.LogInfo("backups_query", "Querying backup tables from database" + (table != null ? " for table: " + table : ""));
                
                var backups = _databaseManager.GetBackupTables(table);

                _logger.LogInfo("backups_query", "Retrieved " + backups.Count + " backup entries");

                return Ok(new BackupsResponse
                {
                    Backups = backups,
                    Count = backups.Count,
                    Table = table
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve backups: " + ex.Message;
                _logger.LogError("backups_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/rollback")]
        public ActionResult<object> RollbackTable([FromBody] RollbackRequest request)
        {
            if (request == null || string.IsNullOrEmpty(request.Table) || !request.VersionId.HasValue)
            {
                return BadRequest(new { error = "Table name and version ID are required for rollback" });
            }

            try
            {
                _logger.LogInfo("rollback_start", "Starting rollback for table " + request.Table + " to version " + request.VersionId);

                // Perform real rollback operation
                var rollbackResult = _databaseManager.RollbackTableToVersion(request.Table, request.VersionId.Value);

                _logger.LogInfo("rollback_complete", "Completed rollback for table " + request.Table + " to version " + request.VersionId);

                return Ok(new 
                { 
                    message = "Rollback completed successfully",
                    table = rollbackResult["table"],
                    version = rollbackResult["version_id"],
                    main_rows = rollbackResult["main_rows_restored"],
                    stage_rows = rollbackResult["stage_rows_cleared"],
                    current_backup_rows = rollbackResult["current_rows_backed_up"],
                    timestamp = rollbackResult["timestamp"]
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Rollback failed for table " + request.Table + ": " + ex.Message;
                _logger.LogError("rollback_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/backups/{tableName}/rollback/{versionId}")]
        public ActionResult<object> RollbackTableRESTful(string tableName, int versionId)
        {
            if (string.IsNullOrEmpty(tableName) || versionId <= 0)
            {
                return BadRequest(new { error = "Table name and valid version ID are required for rollback" });
            }

            try
            {
                _logger.LogInfo("rollback_start", "Starting rollback for table " + tableName + " to version " + versionId);

                // Perform real rollback operation
                var rollbackResult = _databaseManager.RollbackTableToVersion(tableName, versionId);

                _logger.LogInfo("rollback_complete", "Completed rollback for table " + tableName + " to version " + versionId);

                return Ok(new 
                { 
                    message = "Rollback completed successfully",
                    table = rollbackResult["table"],
                    version = rollbackResult["version_id"],
                    main_rows = rollbackResult["main_rows_restored"],
                    stage_rows = rollbackResult["stage_rows_cleared"],
                    current_backup_rows = rollbackResult["current_rows_backed_up"],
                    timestamp = rollbackResult["timestamp"]
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Rollback failed for table " + tableName + ": " + ex.Message;
                _logger.LogError("rollback_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/upload")]
        public ActionResult<object> UploadFile(
            IFormFile file, 
            [FromForm] string header_delimiter = "|",
            [FromForm] string column_delimiter = "|", 
            [FromForm] string row_delimiter = "|\"\"\\r\\n",
            [FromForm] string text_qualifier = "\"",
            [FromForm] int skip_lines = 0,
            [FromForm] string trailer_line = null,
            [FromForm] string load_mode = "full",
            [FromForm] string override_load_type = null,
            [FromForm] bool config_reference_data = false,
            [FromForm] string target_schema = "ref")
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest(new { error = "No file uploaded" });
            }

            // Validate file type
            if (!file.FileName.ToLower().EndsWith(".csv"))
            {
                return BadRequest(new { error = "Only CSV files are supported" });
            }

            try
            {
                // Validate file size
                var maxSize = int.Parse(ExpandEnvironmentVariables(_configuration["FileSettings:MaxUploadSize"]) ?? "20971520"); // 20MB default
                if (file.Length > maxSize)
                {
                    return BadRequest(new { error = "File size exceeds maximum limit of " + maxSize + " bytes" });
                }

                // Generate progress key from filename
                var progressKey = System.Text.RegularExpressions.Regex.Replace(file.FileName, @"[^a-zA-Z0-9_]", "_");
                
                // Save uploaded file temporarily
                var tempPath = Path.Combine(Path.GetTempPath(), progressKey + "_" + file.FileName);
                
                using (var stream = new FileStream(tempPath, FileMode.Create))
                {
                    file.CopyTo(stream);
                }

                _logger.LogInfo("file_uploaded", "File uploaded successfully: " + file.FileName + " (" + file.Length + " bytes)");

                // Create format configuration
                var formatConfig = new Dictionary<string, string>
                {
                    {"header_delimiter", header_delimiter},
                    {"column_delimiter", column_delimiter}, 
                    {"row_delimiter", row_delimiter},
                    {"text_qualifier", text_qualifier},
                    {"skip_lines", skip_lines.ToString()},
                    {"load_mode", load_mode}
                };
                
                if (!string.IsNullOrEmpty(trailer_line))
                {
                    formatConfig["trailer_line"] = trailer_line;
                }
                
                if (!string.IsNullOrEmpty(override_load_type))
                {
                    formatConfig["override_load_type"] = override_load_type;
                }

                // Extract base table name from filename (remove timestamp and .csv extension)
                var baseTableName = ExtractTableNameFromFilename(file.FileName);
                
                // Start background processing using Thread instead of Task (for .NET Framework 4.5 compatibility)
                var processingThread = new System.Threading.Thread(() =>
                {
                    _dataIngestion.ProcessFileBackground(tempPath, baseTableName, target_schema, 
                        formatConfig, config_reference_data, progressKey);
                });
                
                processingThread.IsBackground = true;
                processingThread.Start();

                return Ok(new 
                { 
                    message = "File uploaded successfully", 
                    filename = file.FileName,
                    file_size = file.Length,
                    status = "processing",
                    progress_key = progressKey
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Upload failed: " + ex.Message;
                _logger.LogError("upload_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpGet("/reference-data-config")]
        public ActionResult<object> GetReferenceDataConfig()
        {
            try
            {
                var referenceDataConfig = _databaseManager.GetReferenceDataConfig();
                
                _logger.LogInfo("reference_data_config_query", "Retrieved " + referenceDataConfig.Count + " Reference_Data_Cfg records");
                
                return Ok(new 
                { 
                    data = referenceDataConfig,
                    count = referenceDataConfig.Count
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve Reference_Data_Cfg records: " + ex.Message;
                _logger.LogError("reference_data_config_query", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpGet("/tables/all")]
        public ActionResult<object> GetAllTablesWithSchemas()
        {
            try
            {
                var allTables = _databaseManager.GetAllTablesWithSchemas();
                
                _logger.LogInfo("all_tables_query", "Retrieved " + allTables.Count + " total tables for validation");
                
                return Ok(new 
                { 
                    tables = allTables,
                    count = allTables.Count
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to retrieve all tables: " + ex.Message;
                _logger.LogError("all_tables_query", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/verify-load-type")]
        public ActionResult<object> VerifyLoadType([FromForm] string filename, [FromForm] string load_mode)
        {
            if (string.IsNullOrEmpty(filename) || string.IsNullOrEmpty(load_mode))
            {
                return BadRequest(new { error = "Filename and load_mode are required" });
            }

            try
            {
                var verification = _databaseManager.VerifyLoadType(filename, load_mode);
                
                _logger.LogInfo("load_type_verification", "Verified load type for " + filename + ": mismatch=" + verification["has_mismatch"]);
                
                return Ok(verification);
            }
            catch (Exception ex)
            {
                var errorMsg = "Failed to verify load type: " + ex.Message;
                _logger.LogError("load_type_verification", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        // Helper method to extract base table name from filename
        private string ExtractTableNameFromFilename(string filename)
        {
            if (string.IsNullOrEmpty(filename))
                return filename;
            
            // Remove .csv extension first
            var nameWithoutExtension = filename;
            if (nameWithoutExtension.ToLower().EndsWith(".csv"))
            {
                nameWithoutExtension = nameWithoutExtension.Substring(0, nameWithoutExtension.Length - 4);
            }
            
            // Pattern: [a-zA-Z0-9_-]*[._-]{date|time|timestamp}
            // Extract everything before the first date/timestamp pattern
            var regex = new System.Text.RegularExpressions.Regex(@"^([a-zA-Z0-9_-]+)[._-](\d{8}|\d{4}-\d{2}-\d{2}|\d{14}|\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})");
            var match = regex.Match(nameWithoutExtension);
            
            if (match.Success)
            {
                return match.Groups[1].Value;
            }
            
            // If no timestamp pattern found, return the name without extension
            return nameWithoutExtension;
        }

        // Helper method to get client IP - .NET Framework 4.5 compatible
        private string GetClientIpAddress()
        {
            var xForwardedFor = Request.Headers["X-Forwarded-For"].ToString();
            if (!string.IsNullOrEmpty(xForwardedFor))
            {
                return xForwardedFor.Split(',')[0].Trim();
            }

            var xRealIp = Request.Headers["X-Real-IP"].ToString();
            if (!string.IsNullOrEmpty(xRealIp))
            {
                return xRealIp;
            }

            var remoteIp = Request.HttpContext.Connection.RemoteIpAddress;
            return remoteIp != null ? remoteIp.ToString() : "unknown";
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
            catch (Exception)
            {
                return input; // Return original string if expansion fails
            }
        }
    }
}
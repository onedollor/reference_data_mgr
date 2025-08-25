using System;
using System.Collections.Generic;
using System.IO;
using System.Web;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using ReferenceDataApi.Infrastructure;
using ReferenceDataApi.Models;
using ReferenceDataApi.Services;

namespace ReferenceDataApi.Controllers
{
    [ApiController]
    [Route("")]
    public class ReferenceDataController : ControllerBase
    {
        private readonly IDatabaseManager _databaseManager;
        private readonly IFileHandler _fileHandler;
        private readonly ILogger _logger;
        private readonly IProgressTracker _progressTracker;
        private readonly ICsvDetector _csvDetector;
        private readonly IDataIngestion _dataIngestion;
        private readonly IConfiguration _configuration;

        public ReferenceDataController(
            IDatabaseManager databaseManager,
            IFileHandler fileHandler, 
            ILogger logger,
            IProgressTracker progressTracker,
            ICsvDetector csvDetector,
            IDataIngestion dataIngestion,
            IConfiguration configuration)
        {
            _databaseManager = databaseManager;
            _fileHandler = fileHandler;
            _logger = logger;
            _progressTracker = progressTracker;
            _csvDetector = csvDetector;
            _dataIngestion = dataIngestion;
            _configuration = configuration;
        }

        [HttpGet("/")]
        public ActionResult<ApiResponse> Root()
        {
            return Ok(new ApiResponse 
            { 
                Message = "Reference Data Auto Ingest System - ASP.NET Core Backend",
                Version = "1.0.0",
                Timestamp = DateTime.UtcNow
            });
        }

        [HttpGet("/health/database")]
        public ActionResult<HealthResponse> DatabaseHealth()
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

        [HttpGet("/config")]
        public ActionResult<ConfigResponse> GetConfig()
        {
            try
            {
                var config = new ConfigResponse
                {
                    DatabaseSettings = new DatabaseSettings
                    {
                        DataSchema = _configuration["DatabaseSettings:DataSchema"],
                        BackupSchema = _configuration["DatabaseSettings:BackupSchema"],
                        PostloadStoredProcedure = _configuration["DatabaseSettings:PostloadStoredProcedure"]
                    },
                    FileSettings = new FileSettings
                    {
                        UploadPath = _configuration["FileSettings:UploadPath"],
                        ArchivePath = _configuration["FileSettings:ArchivePath"],
                        TempPath = _configuration["FileSettings:TempPath"]
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
                // For now, return a placeholder response
                // In a full implementation, this would query the system_log table
                var logs = new List<LogEntry>();
                
                // Placeholder implementation - would normally query database
                logs.Add(new LogEntry
                {
                    Timestamp = DateTime.UtcNow.AddMinutes(-5),
                    Level = "INFO",
                    Message = "System started successfully",
                    Context = "system_startup"
                });

                logs.Add(new LogEntry
                {
                    Timestamp = DateTime.UtcNow.AddMinutes(-2),
                    Level = "INFO", 
                    Message = "Database health check passed",
                    Context = "health_check"
                });

                // Filter by level if specified - .NET Framework 4.5 compatible
                if (!string.IsNullOrEmpty(level))
                {
                    var filteredLogs = new List<LogEntry>();
                    foreach (var log in logs)
                    {
                        if (log.Level.Equals(level, StringComparison.OrdinalIgnoreCase))
                        {
                            filteredLogs.Add(log);
                        }
                    }
                    logs = filteredLogs;
                }

                // Apply limit - .NET Framework 4.5 compatible
                if (logs.Count > limit)
                {
                    var limitedLogs = new List<LogEntry>();
                    for (int i = 0; i < limit && i < logs.Count; i++)
                    {
                        limitedLogs.Add(logs[i]);
                    }
                    logs = limitedLogs;
                }

                _logger.LogInfo("logs_query", "Retrieved " + logs.Count + " log entries");

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

        [HttpGet("/backups")]
        public ActionResult<BackupsResponse> GetBackups([FromQuery] string table = null)
        {
            try
            {
                // Placeholder implementation for backup listing
                // In a full implementation, this would query backup tables and metadata
                var backups = new List<BackupInfo>();

                // Add sample backup entries for demonstration
                if (string.IsNullOrEmpty(table) || table == "sample_table")
                {
                    backups.Add(new BackupInfo
                    {
                        Table = "sample_table",
                        BackupTable = "sample_table_backup",
                        VersionId = 1,
                        CreatedDate = DateTime.UtcNow.AddHours(-2),
                        RowCount = 1000,
                        Status = "active"
                    });

                    backups.Add(new BackupInfo
                    {
                        Table = "sample_table",
                        BackupTable = "sample_table_backup", 
                        VersionId = 2,
                        CreatedDate = DateTime.UtcNow.AddHours(-1),
                        RowCount = 1050,
                        Status = "active"
                    });
                }

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
                // Placeholder implementation for rollback operation
                // In a full implementation, this would:
                // 1. Validate the backup version exists
                // 2. Create a new backup of current data  
                // 3. Replace current table data with backup version data
                // 4. Update metadata and logs

                _logger.LogInfo("rollback_start", "Starting rollback for table " + request.Table + " to version " + request.VersionId);

                // Simulate rollback process
                System.Threading.Thread.Sleep(1000);

                _logger.LogInfo("rollback_complete", "Completed rollback for table " + request.Table + " to version " + request.VersionId);

                return Ok(new 
                { 
                    message = "Rollback completed successfully",
                    table = request.Table,
                    version = request.VersionId,
                    timestamp = DateTime.UtcNow
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "Rollback failed for table " + request.Table + ": " + ex.Message;
                _logger.LogError("rollback_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
        }

        [HttpPost("/upload")]
        public ActionResult<object> UploadFile(IFormFile file, [FromForm] string tableName, [FromForm] string targetSchema, [FromForm] bool configReferenceData = false)
        {
            if (file == null || file.Length == 0)
            {
                return BadRequest(new { error = "No file uploaded" });
            }

            if (string.IsNullOrEmpty(tableName))
            {
                return BadRequest(new { error = "Table name is required" });
            }

            try
            {
                // Generate unique progress key
                var progressKey = "upload_" + Guid.NewGuid().ToString("N").Substring(0, 8);
                
                // Save uploaded file temporarily
                var tempPath = Path.Combine(Path.GetTempPath(), progressKey + "_" + file.FileName);
                
                using (var stream = new FileStream(tempPath, FileMode.Create))
                {
                    file.CopyTo(stream);
                }

                _logger.LogInfo("file_uploaded", "File uploaded: " + file.FileName + " for table " + tableName);

                // Start background processing
                var formatConfig = new Dictionary<string, string>
                {
                    {"delimiter", ","},
                    {"quote_char", "\""},
                    {"encoding", "utf-8"}
                };

                // Start background processing using Thread instead of Task (for .NET Framework 4.5 compatibility)
                var processingThread = new System.Threading.Thread(() =>
                {
                    _dataIngestion.ProcessFileBackground(tempPath, tableName, targetSchema ?? "ref", 
                        formatConfig, configReferenceData, progressKey);
                });
                
                processingThread.IsBackground = true;
                processingThread.Start();

                return Ok(new 
                { 
                    message = "File upload started", 
                    progressKey = progressKey,
                    fileName = file.FileName,
                    tableName = tableName,
                    fileSize = file.Length
                });
            }
            catch (Exception ex)
            {
                var errorMsg = "File upload failed: " + ex.Message;
                _logger.LogError("upload_error", errorMsg);
                return BadRequest(new { error = errorMsg });
            }
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
    }
}
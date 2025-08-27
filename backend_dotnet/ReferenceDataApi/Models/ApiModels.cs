using System;
using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace ReferenceDataApi.Models
{
    public class ApiResponse
    {
        public string Message { get; set; }
        public string Version { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class HealthResponse
    {
        public string Status { get; set; }
        public string Database { get; set; }
        public DateTime Timestamp { get; set; }
        public string Message { get; set; }
    }

    public class ConfigResponse
    {
        public int max_upload_size { get; set; }
        public List<string> supported_formats { get; set; }
        public DefaultDelimiters default_delimiters { get; set; }
        public DelimiterOptions delimiter_options { get; set; }
        public DatabaseSettings DatabaseSettings { get; set; }
        public FileSettings FileSettings { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class DatabaseSettings
    {
        public string DataSchema { get; set; }
        public string BackupSchema { get; set; }
        public string PostloadStoredProcedure { get; set; }
    }

    public class FileSettings
    {
        public string UploadPath { get; set; }
        public string ArchivePath { get; set; }
        public string TempPath { get; set; }
    }

    public class DelimiterOptions
    {
        public List<string> header_delimiter { get; set; }
        public List<string> column_delimiter { get; set; }
        public List<string> row_delimiter { get; set; }
        public List<string> text_qualifier { get; set; }
    }

    public class DefaultDelimiters
    {
        public string header_delimiter { get; set; }
        public string column_delimiter { get; set; }
        public string row_delimiter { get; set; }
        public string text_qualifier { get; set; }
    }

    public class SchemasResponse
    {
        public List<string> Schemas { get; set; }
        public string DefaultSchema { get; set; }
        public int Count { get; set; }
    }

    public class FeaturesResponse
    {
        public bool AsyncIngestion { get; set; }
        public bool ProgressTracking { get; set; }
        public bool BackupSupport { get; set; }
        public bool SchemaMatching { get; set; }
        public bool CsvDetection { get; set; }
        public bool RollbackSupport { get; set; }
        public bool LoggingEnabled { get; set; }
    }

    public class FormatDetectionResponse
    {
        public bool Detected { get; set; }
        public double Confidence { get; set; }
        public CsvFormatSuggestion Suggestions { get; set; }
        public DetectedFormat detected_format { get; set; }
        public FormatAnalysis Analysis { get; set; }
        public string Message { get; set; }
    }

    public class DetectedFormat
    {
        public string header_delimiter { get; set; }
        public string column_delimiter { get; set; }
        public string row_delimiter { get; set; }
        public string text_qualifier { get; set; }
        public int skip_lines { get; set; }
        public string trailer_line { get; set; }
        public string load_mode { get; set; }
    }

    public class CsvFormatSuggestion
    {
        public string HeaderDelimiter { get; set; }
        public string ColumnDelimiter { get; set; }
        public string RowDelimiter { get; set; }
        public string TextQualifier { get; set; }
        public int SkipLines { get; set; }
        public string TrailerLine { get; set; }
        public string LoadMode { get; set; }
    }

    public class FormatAnalysis
    {
        public int SampleSize { get; set; }
        public List<string> DetectedDelimiters { get; set; }
        public List<string> DetectedEncodings { get; set; }
        public bool HasHeader { get; set; }
        public bool HasTrailer { get; set; }
        public int EstimatedRows { get; set; }
        public int EstimatedColumns { get; set; }
        public List<string> Columns { get; set; }
        public List<List<string>> SampleRows { get; set; }
    }

    public class TableColumn
    {
        public string Name { get; set; }
        public string DataType { get; set; }
        public int? MaxLength { get; set; }
        public int? NumericPrecision { get; set; }
        public int? NumericScale { get; set; }
        public bool Nullable { get; set; }
        public string DefaultValue { get; set; }
        public int Position { get; set; }
    }

    public class TableInfo
    {
        public string Schema { get; set; }
        public string Table { get; set; }
        public string FullName { get; set; }
    }

    public class SchemaMatchRequest
    {
        public List<string> Columns { get; set; }
    }


    public class LogEntry
    {
        public DateTime Timestamp { get; set; }
        public string Level { get; set; }
        public string Message { get; set; }
        public string Context { get; set; }
        public string Category { get; set; }
        public string Source { get; set; }
    }

    public class LogResponse
    {
        public List<LogEntry> Logs { get; set; }
        public int Count { get; set; }
        public DateTime LastUpdated { get; set; }
    }

    public class ProgressInfo
    {
        public string Key { get; set; }
        public bool Found { get; set; }
        public bool Done { get; set; }
        public bool Canceled { get; set; }
        public string Error { get; set; }
        public string Stage { get; set; }
        public int Progress { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class TablesResponse
    {
        public List<TableInfo> Tables { get; set; }
        public int Count { get; set; }
    }

    public class SchemaMatchResponse
    {
        public List<TableInfo> MatchingTables { get; set; }
        public List<string> InputColumns { get; set; }
        public int Count { get; set; }
    }

    public class ProgressResponse
    {
        public string Key { get; set; }
        public bool Found { get; set; }
        public bool Done { get; set; }
        public bool Canceled { get; set; }
        public string Error { get; set; }
        public string Stage { get; set; }
        public int Progress { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; }
    }

    public class LogsResponse
    {
        public List<LogEntry> Logs { get; set; }
        public int Count { get; set; }
        public int Limit { get; set; }
        public string Level { get; set; }
    }

    public class BackupsResponse
    {
        public List<BackupInfo> Backups { get; set; }
        public int Count { get; set; }
        public string Table { get; set; }
    }

    public class BackupInfo
    {
        [JsonPropertyName("table")]
        public string Table { get; set; }
        
        [JsonPropertyName("backup_table")]
        public string BackupTable { get; set; }
        
        [JsonPropertyName("version_id")]
        public int VersionId { get; set; }
        
        [JsonPropertyName("created_date")]
        public DateTime CreatedDate { get; set; }
        
        [JsonPropertyName("row_count")]
        public int RowCount { get; set; }
        
        [JsonPropertyName("status")]
        public string Status { get; set; }
        
        [JsonPropertyName("base_name")]
        public string BaseName { get; set; }
        
        [JsonPropertyName("has_main")]
        public bool HasMain { get; set; }
        
        [JsonPropertyName("has_stage")]
        public bool HasStage { get; set; }
        
        [JsonPropertyName("version_count")]
        public int VersionCount { get; set; }
        
        [JsonPropertyName("latest_version")]
        public int? LatestVersion { get; set; }
    }

    public class RollbackRequest
    {
        public string Table { get; set; }
        public int? VersionId { get; set; }
    }
}
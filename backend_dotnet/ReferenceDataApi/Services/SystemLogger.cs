using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Microsoft.Extensions.Configuration;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public class SystemLogger : ILogger
    {
        private readonly string _logDirectory;
        private readonly object _lockObject = new object();

        public SystemLogger(IConfiguration configuration)
        {
            _logDirectory = Path.Combine(Directory.GetCurrentDirectory(), "logs");
            
            // Ensure log directory exists - .NET Framework 4.5 compatible
            if (!Directory.Exists(_logDirectory))
            {
                Directory.CreateDirectory(_logDirectory);
            }
        }

        public void LogInfo(string category, string message)
        {
            WriteLog("INFO", category, message);
        }

        public void LogWarning(string category, string message)
        {
            WriteLog("WARNING", category, message);
        }

        public void LogError(string category, string message)
        {
            WriteLog("ERROR", category, message);
        }

        public List<LogEntry> GetLogs(int limit = 100, string logType = null)
        {
            var logs = new List<LogEntry>();
            
            try
            {
                var logFile = Path.Combine(_logDirectory, "system.log");
                
                if (!File.Exists(logFile))
                    return logs;

                // .NET Framework 4.5 compatible file reading
                var lines = File.ReadAllLines(logFile);
                
                // Parse log entries - simple format: TIMESTAMP [LEVEL] CATEGORY: MESSAGE
                foreach (var line in lines.Reverse().Take(limit))
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;
                        
                    try
                    {
                        var parts = line.Split(new char[] { ' ' }, 4, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length >= 4)
                        {
                            var timestamp = DateTime.Parse(parts[0] + " " + parts[1]);
                            var level = parts[2].Trim('[', ']');
                            var categoryAndMessage = parts[3];
                            
                            var colonIndex = categoryAndMessage.IndexOf(": ");
                            var category = colonIndex > 0 ? categoryAndMessage.Substring(0, colonIndex) : "general";
                            var message = colonIndex > 0 ? categoryAndMessage.Substring(colonIndex + 2) : categoryAndMessage;
                            
                            // Filter by log type if specified
                            if (string.IsNullOrEmpty(logType) || level.ToLower() == logType.ToLower())
                            {
                                logs.Add(new LogEntry
                                {
                                    Timestamp = timestamp,
                                    Level = level,
                                    Category = category,
                                    Message = message,
                                    Source = "system"
                                });
                            }
                        }
                    }
                    catch
                    {
                        // Skip malformed log lines
                        continue;
                    }
                }
            }
            catch (Exception)
            {
                // Return empty list if file reading fails
                return logs;
            }
            
            return logs;
        }

        public void RotateLogs(int maxSizeMb = 10)
        {
            try
            {
                var logFile = Path.Combine(_logDirectory, "system.log");
                
                if (!File.Exists(logFile))
                    return;
                    
                var fileInfo = new FileInfo(logFile);
                var maxSizeBytes = maxSizeMb * 1024 * 1024;
                
                if (fileInfo.Length > maxSizeBytes)
                {
                    lock (_lockObject)
                    {
                        // Create backup with timestamp - .NET Framework 4.5 compatible
                        var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                        var backupFile = Path.Combine(_logDirectory, "system_" + timestamp + ".log");
                        
                        File.Copy(logFile, backupFile);
                        File.Delete(logFile);
                        
                        WriteLog("INFO", "log_rotation", "Log rotated. Backup created: " + Path.GetFileName(backupFile));
                    }
                }
            }
            catch (Exception ex)
            {
                // Log rotation failure - write to console as fallback
                Console.WriteLine("Log rotation failed: " + ex.Message);
            }
        }

        private void WriteLog(string level, string category, string message)
        {
            try
            {
                lock (_lockObject)
                {
                    var logFile = Path.Combine(_logDirectory, "system.log");
                    var timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss");
                    var logEntry = timestamp + " [" + level + "] " + category + ": " + message;
                    
                    // .NET Framework 4.5 compatible file writing
                    using (var writer = new StreamWriter(logFile, true))
                    {
                        writer.WriteLine(logEntry);
                    }
                }
            }
            catch
            {
                // If file writing fails, write to console as fallback
                Console.WriteLine("[" + level + "] " + category + ": " + message);
            }
        }
    }
}
using System;
using System.Collections.Generic;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public interface ILogger
    {
        void LogInfo(string category, string message);
        void LogWarning(string category, string message);
        void LogError(string category, string message);
        List<LogEntry> GetLogs(int limit = 100, string logType = null);
        void RotateLogs(int maxSizeMb = 10);
    }
}
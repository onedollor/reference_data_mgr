using System;
using System.Collections.Concurrent;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public class ProgressTracker : IProgressTracker
    {
        // .NET Framework 4.5 compatible concurrent dictionary
        private readonly ConcurrentDictionary<string, ProgressInfo> _progressData;

        public ProgressTracker()
        {
            _progressData = new ConcurrentDictionary<string, ProgressInfo>();
        }

        public string CreateProgressKey()
        {
            // .NET Framework 4.5 compatible GUID generation
            var key = Guid.NewGuid().ToString("N").Substring(0, 8);
            
            _progressData.TryAdd(key, new ProgressInfo
            {
                Key = key,
                Found = true,
                Done = false,
                Canceled = false,
                Error = null,
                Stage = "initialized",
                Progress = 0,
                Message = "Process initialized",
                Timestamp = DateTime.UtcNow
            });
            
            return key;
        }

        public void UpdateProgress(string key, string stage, int progress = 0, string message = "")
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            _progressData.AddOrUpdate(key, 
                new ProgressInfo
                {
                    Key = key,
                    Found = true,
                    Done = false,
                    Canceled = false,
                    Error = null,
                    Stage = stage,
                    Progress = progress,
                    Message = message,
                    Timestamp = DateTime.UtcNow
                },
                (k, existing) =>
                {
                    existing.Stage = stage;
                    existing.Progress = progress;
                    existing.Message = message;
                    existing.Timestamp = DateTime.UtcNow;
                    return existing;
                });
        }

        public void MarkDone(string key)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            _progressData.AddOrUpdate(key,
                new ProgressInfo
                {
                    Key = key,
                    Found = true,
                    Done = true,
                    Canceled = false,
                    Error = null,
                    Stage = "completed",
                    Progress = 100,
                    Message = "Process completed successfully",
                    Timestamp = DateTime.UtcNow
                },
                (k, existing) =>
                {
                    existing.Done = true;
                    existing.Stage = "completed";
                    existing.Progress = 100;
                    existing.Message = "Process completed successfully";
                    existing.Timestamp = DateTime.UtcNow;
                    return existing;
                });
        }

        public void MarkCanceled(string key)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            _progressData.AddOrUpdate(key,
                new ProgressInfo
                {
                    Key = key,
                    Found = true,
                    Done = false,
                    Canceled = true,
                    Error = "Process was canceled by user",
                    Stage = "canceled",
                    Progress = 0,
                    Message = "Process canceled",
                    Timestamp = DateTime.UtcNow
                },
                (k, existing) =>
                {
                    existing.Canceled = true;
                    existing.Error = "Process was canceled by user";
                    existing.Stage = "canceled";
                    existing.Message = "Process canceled";
                    existing.Timestamp = DateTime.UtcNow;
                    return existing;
                });
        }

        public void MarkError(string key, string error)
        {
            if (string.IsNullOrEmpty(key))
                return;
                
            _progressData.AddOrUpdate(key,
                new ProgressInfo
                {
                    Key = key,
                    Found = true,
                    Done = false,
                    Canceled = false,
                    Error = error,
                    Stage = "error",
                    Progress = 0,
                    Message = "Process failed: " + error,
                    Timestamp = DateTime.UtcNow
                },
                (k, existing) =>
                {
                    existing.Error = error;
                    existing.Stage = "error";
                    existing.Message = "Process failed: " + error;
                    existing.Timestamp = DateTime.UtcNow;
                    return existing;
                });
        }

        public ProgressInfo GetProgress(string key)
        {
            if (string.IsNullOrEmpty(key))
            {
                return new ProgressInfo
                {
                    Key = key,
                    Found = false,
                    Done = false,
                    Canceled = false,
                    Error = null,
                    Stage = "not_found",
                    Progress = 0,
                    Message = "Progress key not found",
                    Timestamp = DateTime.UtcNow
                };
            }
            
            ProgressInfo progressInfo;
            if (_progressData.TryGetValue(key, out progressInfo))
            {
                return progressInfo;
            }
            
            return new ProgressInfo
            {
                Key = key,
                Found = false,
                Done = false,
                Canceled = false,
                Error = null,
                Stage = "not_found",
                Progress = 0,
                Message = "Progress key not found",
                Timestamp = DateTime.UtcNow
            };
        }

        public bool IsProgressKey(string key)
        {
            if (string.IsNullOrEmpty(key))
                return false;
                
            return _progressData.ContainsKey(key);
        }

        public bool IsCanceled(string key)
        {
            if (string.IsNullOrEmpty(key))
                return false;
                
            var progress = GetProgress(key);
            return progress.Found && progress.Canceled;
        }

        public bool CancelOperation(string key)
        {
            if (string.IsNullOrEmpty(key))
                return false;
                
            if (!_progressData.ContainsKey(key))
                return false;
                
            MarkCanceled(key);
            return true;
        }
    }
}
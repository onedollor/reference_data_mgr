using System;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public interface IProgressTracker
    {
        string CreateProgressKey();
        void UpdateProgress(string key, string stage, int progress = 0, string message = "");
        void MarkDone(string key);
        void MarkCanceled(string key);
        void MarkError(string key, string error);
        ProgressInfo GetProgress(string key);
        bool IsProgressKey(string key);
        bool IsCanceled(string key);
        bool CancelOperation(string key);
    }
}
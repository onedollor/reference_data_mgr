using System;
using Microsoft.AspNetCore.Http;

namespace ReferenceDataApi.Services
{
    public interface IFileHandler
    {
        string SaveUploadedFile(IFormFile file);
        void EnsureDirectoriesExist();
        string MoveToArchive(string filePath, string fileName);
        void CleanupTempFiles();
        bool ValidateFileExtension(string fileName);
        long GetFileSize(string filePath);
    }
}
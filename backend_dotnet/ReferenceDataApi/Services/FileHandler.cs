using System;
using System.IO;
using System.Linq;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;

namespace ReferenceDataApi.Services
{
    public class FileHandler : IFileHandler
    {
        private readonly string _uploadPath;
        private readonly string _archivePath;
        private readonly string _tempPath;
        private readonly ILogger _logger;
        
        private readonly string[] _allowedExtensions = { ".csv", ".txt", ".tsv" };

        public FileHandler(IConfiguration configuration, ILogger logger)
        {
            _uploadPath = configuration["FileSettings:UploadPath"] ?? "./uploads";
            _archivePath = configuration["FileSettings:ArchivePath"] ?? "./archive";  
            _tempPath = configuration["FileSettings:TempPath"] ?? "./temp";
            _logger = logger;
            
            EnsureDirectoriesExist();
        }

        public void EnsureDirectoriesExist()
        {
            try
            {
                // .NET Framework 4.5 compatible directory creation
                if (!Directory.Exists(_uploadPath))
                    Directory.CreateDirectory(_uploadPath);
                    
                if (!Directory.Exists(_archivePath))
                    Directory.CreateDirectory(_archivePath);
                    
                if (!Directory.Exists(_tempPath))
                    Directory.CreateDirectory(_tempPath);
                    
                _logger.LogInfo("file_handler_init", "File directories initialized successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError("file_handler_init_error", "Failed to create file directories: " + ex.Message);
                throw new Exception("Failed to initialize file directories: " + ex.Message);
            }
        }

        public string SaveUploadedFile(IFormFile file)
        {
            if (file == null || file.Length == 0)
            {
                throw new ArgumentException("No file provided or file is empty");
            }

            if (!ValidateFileExtension(file.FileName))
            {
                throw new ArgumentException("File type not allowed. Only CSV, TXT, and TSV files are supported.");
            }

            try
            {
                // Generate unique filename - .NET Framework 4.5 compatible
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var originalFileName = Path.GetFileNameWithoutExtension(file.FileName);
                var extension = Path.GetExtension(file.FileName);
                var uniqueFileName = originalFileName + "_" + timestamp + extension;
                
                var filePath = Path.Combine(_uploadPath, uniqueFileName);
                
                // Save file - .NET Framework 4.5 compatible
                using (var stream = new FileStream(filePath, FileMode.Create))
                {
                    file.CopyTo(stream);
                }
                
                _logger.LogInfo("file_uploaded", "File saved successfully: " + uniqueFileName + " (" + file.Length + " bytes)");
                
                return filePath;
            }
            catch (Exception ex)
            {
                _logger.LogError("file_upload_error", "Failed to save uploaded file: " + ex.Message);
                throw new Exception("Failed to save uploaded file: " + ex.Message);
            }
        }

        public string MoveToArchive(string filePath, string fileName)
        {
            try
            {
                if (!File.Exists(filePath))
                {
                    throw new FileNotFoundException("Source file not found: " + filePath);
                }
                
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss");
                var extension = Path.GetExtension(fileName);
                var nameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
                var archiveFileName = nameWithoutExtension + "_processed_" + timestamp + extension;
                
                var archiveFilePath = Path.Combine(_archivePath, archiveFileName);
                
                // Move file to archive - .NET Framework 4.5 compatible
                File.Move(filePath, archiveFilePath);
                
                _logger.LogInfo("file_archived", "File moved to archive: " + archiveFileName);
                
                return archiveFilePath;
            }
            catch (Exception ex)
            {
                _logger.LogError("file_archive_error", "Failed to move file to archive: " + ex.Message);
                throw new Exception("Failed to archive file: " + ex.Message);
            }
        }

        public void CleanupTempFiles()
        {
            try
            {
                if (!Directory.Exists(_tempPath))
                    return;
                    
                var tempFiles = Directory.GetFiles(_tempPath);
                var cutoffTime = DateTime.Now.AddHours(-24); // Delete files older than 24 hours
                
                var deletedCount = 0;
                foreach (var file in tempFiles)
                {
                    try
                    {
                        var fileInfo = new FileInfo(file);
                        if (fileInfo.CreationTime < cutoffTime)
                        {
                            File.Delete(file);
                            deletedCount++;
                        }
                    }
                    catch
                    {
                        // Skip files that can't be deleted
                        continue;
                    }
                }
                
                if (deletedCount > 0)
                {
                    _logger.LogInfo("temp_cleanup", "Cleaned up " + deletedCount + " temporary files");
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning("temp_cleanup_error", "Temporary file cleanup failed: " + ex.Message);
            }
        }

        public bool ValidateFileExtension(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
                return false;
                
            var extension = Path.GetExtension(fileName).ToLowerInvariant();
            
            // .NET Framework 4.5 compatible array contains check
            foreach (var allowedExt in _allowedExtensions)
            {
                if (extension == allowedExt)
                    return true;
            }
            
            return false;
        }

        public long GetFileSize(string filePath)
        {
            try
            {
                if (File.Exists(filePath))
                {
                    var fileInfo = new FileInfo(filePath);
                    return fileInfo.Length;
                }
                return 0;
            }
            catch
            {
                return 0;
            }
        }
    }
}
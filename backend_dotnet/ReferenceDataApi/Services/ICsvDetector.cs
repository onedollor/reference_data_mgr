using ReferenceDataApi.Models;

namespace ReferenceDataApi.Services
{
    public interface ICsvDetector
    {
        FormatDetectionResponse DetectFormat(string filePath);
    }
}
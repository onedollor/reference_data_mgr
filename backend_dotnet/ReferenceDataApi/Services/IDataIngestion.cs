using System.Collections.Generic;

namespace ReferenceDataApi.Services
{
    public interface IDataIngestion
    {
        void ProcessFileBackground(string filePath, string tableName, string targetSchema, 
            Dictionary<string, string> formatConfig, bool configReferenceData, string progressKey);
    }
}
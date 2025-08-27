using System;
using System.Collections.Generic;
using System.Data;

namespace ReferenceDataApi.Infrastructure
{
    public interface IDatabaseManager
    {
        bool TestConnection();
        List<string> GetSchemas();
        List<string> GetTables(string schema);
        DataTable ExecuteQuery(string query);
        int ExecuteNonQuery(string query);
        bool SchemaExists(string schemaName);
        bool TableExists(string schemaName, string tableName);
        Dictionary<string, string> GetTableColumns(string schemaName, string tableName);
    }
}
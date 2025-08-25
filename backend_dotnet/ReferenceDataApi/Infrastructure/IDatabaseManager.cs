using System;
using System.Collections.Generic;
using System.Data;
using ReferenceDataApi.Models;

namespace ReferenceDataApi.Infrastructure
{
    public interface IDatabaseManager
    {
        bool TestConnection();
        List<string> GetAvailableSchemas();
        List<TableColumn> GetTableColumns(string tableName, string schema = null);
        bool TableExists(string tableName, string schema = null);
        void CreateTable(string tableName, List<TableColumn> columns, string schema = null, bool addMetadataColumns = true);
        void CreateBackupTable(string tableName, List<TableColumn> columns);
        List<TableInfo> GetAllTables();
        List<TableInfo> GetSchemaMatchedTables(List<string> fileColumns);
        int BackupExistingData(string sourceTable, string backupTable);
        void DropTableIfExists(string tableName, string schema = null);
        void TruncateTable(string tableName, string schema = null);
        Dictionary<string, object> SyncMainTableColumns(string tableName, List<TableColumn> fileColumns);
        void EnsureReferenceDataCfgTable();
        void EnsurePostloadStoredProcedure();
        void InsertReferenceDataCfgRecord(string tableName);
    }
}
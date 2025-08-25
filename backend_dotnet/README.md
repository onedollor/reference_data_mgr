# Reference Data API - ASP.NET Core Backend

This is the converted ASP.NET Core version of the Python FastAPI backend, using only .NET Framework 4.5 compatible features for maximum compatibility.

## System Requirements

- .NET 6.0+ Runtime (running on Ubuntu 24.04)
- SQL Server with ODBC drivers (already installed)
- Ubuntu 24.04 LTS

## Project Structure

```
backend_dotnet/
├── ReferenceDataApi/
│   ├── Controllers/           # API endpoints
│   │   └── ReferenceDataController.cs
│   ├── Infrastructure/        # Database layer
│   │   ├── IDatabaseManager.cs
│   │   └── DatabaseManager.cs
│   ├── Services/             # Business logic services
│   │   ├── ILogger.cs & SystemLogger.cs
│   │   ├── IFileHandler.cs & FileHandler.cs
│   │   ├── IProgressTracker.cs & ProgressTracker.cs
│   │   ├── ICsvDetector.cs & CsvDetector.cs
│   │   └── IDataIngestion.cs & DataIngestion.cs
│   ├── Models/               # Data models
│   │   └── ApiModels.cs
│   ├── Program.cs           # Application entry point
│   ├── appsettings.json     # Configuration
│   └── ReferenceDataApi.csproj
└── README.md
```

## Configuration

Update `appsettings.json` with your database connection string:

```json
{
  "ConnectionStrings": {
    "DefaultConnection": "Server=your-server;Database=your-db;Integrated Security=false;User ID=user;Password=password;TrustServerCertificate=true;"
  },
  "DatabaseSettings": {
    "DataSchema": "ref",
    "BackupSchema": "bkp", 
    "PostloadStoredProcedure": "sp_ref_postload"
  }
}
```

## Build and Run

```bash
# Navigate to project directory
cd /home/lin/repo/reference_data_mgr/backend_dotnet/ReferenceDataApi

# Restore dependencies
dotnet restore

# Build the project
dotnet build

# Run the application
dotnet run
```

The API will be available at `http://localhost:5000`

## .NET Framework 4.5 Compatibility Features

This implementation uses only features compatible with .NET Framework 4.5:

- **No async/await**: All operations use synchronous patterns
- **No LINQ expressions**: Uses traditional foreach loops and manual operations  
- **No nullable reference types**: Disabled nullable context
- **C# 5.0 language version**: Restricted to older C# features
- **Traditional dependency injection**: Manual service registration
- **Legacy collection operations**: No modern collection methods

## API Endpoints

### Core Endpoints
- `GET /` - API information
- `GET /health/database` - Database health check
- `GET /config` - System configuration
- `GET /features` - Feature flags

### Schema & Tables
- `GET /schemas` - Available database schemas
- `POST /detect-format` - CSV format detection

### Progress Tracking  
- `GET /progress/{key}` - Get progress status
- `POST /progress/{key}/cancel` - Cancel operation

## Key Differences from Python Version

1. **Synchronous Operations**: All database and file operations are synchronous
2. **Manual Memory Management**: Explicit using statements for resources
3. **Traditional Exception Handling**: try/catch instead of Python's exception handling
4. **Type Safety**: Strongly typed throughout with explicit type declarations
5. **Service-Oriented Architecture**: Clear separation of concerns with interfaces

## Development Notes

- All string operations use traditional concatenation instead of string interpolation
- File operations use FileStream and StreamReader/Writer explicitly
- Database operations use SqlConnection and SqlCommand directly
- No modern C# features like pattern matching or switch expressions
- Collections use List<T> and Dictionary<T,K> with traditional iteration

## Future Enhancements

To complete the conversion:

1. Implement remaining DatabaseManager methods
2. Add complete CSV parsing logic in DataIngestion
3. Implement backup and rollback operations  
4. Add comprehensive error handling
5. Complete schema matching functionality
6. Add unit tests

## Compatibility Notes

This codebase should run on any system supporting .NET 6+ while maintaining compatibility with .NET Framework 4.5 coding patterns and practices.
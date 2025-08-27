# Backend .NET - IIS 10 Deployment Guide

## Project Overview

The `backend_dotnet` project has been converted to a traditional ASP.NET Web API project targeting .NET Framework 4.7.2, making it compatible with IIS 10 deployment.

## Key Changes Made

### 1. **Framework Conversion**
- **From**: ASP.NET Core (.NET 6.0) 
- **To**: ASP.NET Web API (.NET Framework 4.7.2)
- **Reason**: Full compatibility with IIS 10 and traditional Windows hosting

### 2. **Project Structure**
```
backend_dotnet/
©À©¤©¤ ReferenceDataApi/
©¦   ©À©¤©¤ App_Start/
©¦   ©¦   ©¸©¤©¤ WebApiConfig.cs           # Web API routing configuration
©¦   ©À©¤©¤ Controllers/
©¦   ©¦   ©¸©¤©¤ ReferenceDataController.cs # Main API controller
©¦   ©À©¤©¤ Infrastructure/
©¦   ©¦   ©À©¤©¤ IDatabaseManager.cs       # Database interface
©¦   ©¦   ©¸©¤©¤ DatabaseManager.cs        # Database implementation
©¦   ©À©¤©¤ Models/
©¦   ©¦   ©¸©¤©¤ ApiModels.cs              # API response models
©¦   ©À©¤©¤ Properties/
©¦   ©¦   ©¸©¤©¤ AssemblyInfo.cs           # Assembly metadata
©¦   ©À©¤©¤ Global.asax                   # Application lifecycle
©¦   ©À©¤©¤ Global.asax.cs               # Application startup
©¦   ©À©¤©¤ Web.config                   # IIS configuration
©¦   ©¸©¤©¤ ReferenceDataApi.csproj      # Project file
©¸©¤©¤ ReferenceDataManager.sln         # Solution file
```

### 3. **Database Connection**
Updated to use the working connection string from your DatabaseTest.cs:
```xml
<connectionStrings>
  <add name="DefaultConnection" 
       connectionString="Server=LIN9400F\SQL2ETL;Database=test;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=30;" />
</connectionStrings>
```

### 4. **API Endpoints**
The API provides these endpoints via conventional routing:

- `GET /api/referencedata` - API information
- `GET /api/referencedata/health` - Database health check  
- `GET /api/referencedata/config` - System configuration
- `GET /api/referencedata/schemas` - Available database schemas
- `GET /api/referencedata/tables/{schemaName}` - Tables in schema
- `GET /api/referencedata/columns/{schemaName}/{tableName}` - Table columns
- `GET /api/referencedata/test` - Connection test

### 5. **IIS 10 Compatibility Features**

#### Web.config Settings:
- **Target Framework**: 4.7.2
- **HTTP Handlers**: Configured for extensionless URLs
- **CORS**: Enabled for cross-origin requests
- **JSON Formatting**: Camel case property names
- **Request Limits**: 50MB upload, 5 minute timeout

#### Application Pool Requirements:
- **.NET Framework Version**: v4.0 (supports 4.7.2)
- **Managed Pipeline Mode**: Integrated
- **Process Identity**: ApplicationPoolIdentity or custom account with database access

## Deployment Instructions

### 1. **Prepare IIS 10**
```powershell
# Enable IIS and ASP.NET features
Enable-WindowsOptionalFeature -Online -FeatureName IIS-WebServerRole
Enable-WindowsOptionalFeature -Online -FeatureName IIS-ASPNET45
```

### 2. **Build and Publish**
```bash
# From backend_dotnet directory
dotnet restore ReferenceDataManager.sln
dotnet build ReferenceDataManager.sln --configuration Release
```

### 3. **Deploy to IIS**
1. Copy bin/ folder contents to IIS application bin/ directory
2. Copy all .aspx, .asax, .config files to IIS application root
3. Ensure App_Data folder exists with proper permissions

### 4. **Configure Application Pool**
```powershell
# Create application pool
New-WebAppPool -Name "ReferenceDataApiPool"
Set-ItemProperty -Path "IIS:\AppPools\ReferenceDataApiPool" -Name processModel.identityType -Value ApplicationPoolIdentity
Set-ItemProperty -Path "IIS:\AppPools\ReferenceDataApiPool" -Name managedRuntimeVersion -Value "v4.0"

# Create IIS application
New-WebApplication -Site "Default Web Site" -Name "referencedata" -PhysicalPath "C:\inetpub\wwwroot\referencedata" -ApplicationPool "ReferenceDataApiPool"
```

### 5. **Test Deployment**
```bash
# Test API endpoints
curl http://localhost/referencedata/api/referencedata
curl http://localhost/referencedata/api/referencedata/health
curl http://localhost/referencedata/api/referencedata/test
```

## Key Differences from Python Version

| Feature | Python FastAPI | .NET Web API |
|---------|----------------|--------------|
| **Framework** | FastAPI/Uvicorn | ASP.NET Web API |
| **Runtime** | Python 3.x | .NET Framework 4.7.2 |
| **Database** | asyncio + ODBC | System.Data.SqlClient |
| **Routing** | Path decorators | Conventional routing |
| **CORS** | FastAPI middleware | Web API CORS |
| **JSON** | Pydantic models | Newtonsoft.Json |
| **Deployment** | Self-hosted/Gunicorn | IIS 10 |

## Configuration Settings

### Database Settings (Web.config appSettings)
- `DataSchema`: ref (default schema for reference data)
- `BackupSchema`: bkp (schema for backup tables)  
- `PostloadStoredProcedure`: sp_ref_postload (post-processing SP)

### Server Settings
- `Host`: localhost (server hostname)
- `HttpPort`: 8000 (HTTP port for development)
- `HttpsPort`: 8001 (HTTPS port for development)

## Troubleshooting

### Common Issues

1. **Assembly Loading Errors**
   - Ensure .NET Framework 4.7.2 is installed
   - Verify application pool targets correct .NET version

2. **Database Connection Failures**
   - Check SQL Server instance is running
   - Verify connection string credentials
   - Ensure application pool identity has database access

3. **HTTP 500 Errors**
   - Check Windows Event Log for detailed error messages
   - Enable detailed error pages in Web.config for debugging

### Logs and Monitoring
- **IIS Logs**: `C:\inetpub\logs\LogFiles\W3SVC1\`
- **Windows Event Log**: Application and System logs
- **SQL Server Logs**: Check SQL Server error logs for connection issues

## Performance Considerations

- **Connection Pooling**: Enabled by default in .NET Framework
- **JSON Serialization**: Optimized for camelCase output
- **Error Handling**: Graceful degradation with structured error responses
- **CORS**: Configured for production cross-origin scenarios

## Security Notes

- **Input Validation**: SQL parameters prevent injection attacks
- **Connection Strings**: Store sensitive data in machine.config or encrypted sections
- **HTTPS**: Configure SSL certificates for production deployment
- **Authentication**: Ready for Windows Authentication or custom schemes

This implementation provides a solid foundation for IIS 10 deployment while maintaining compatibility with the existing database schema and API contract.
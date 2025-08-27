#!/bin/bash

# Start ASP.NET Core Reference Data API
# Compatible with Ubuntu 24.04 and .NET 6

echo "Starting ASP.NET Core Reference Data API..."
echo "System: Ubuntu 24.04 with .NET 6"
echo "Compatibility: .NET Framework 4.5 features only"
echo ""

# Check if .NET is installed
if ! command -v dotnet &> /dev/null; then
    echo "ERROR: .NET runtime not found. Please install .NET 6 or higher."
    echo "Run: sudo apt install -y dotnet-sdk-8.0"
    exit 1
fi

# Display .NET version
echo "Detected .NET version:"
dotnet --version
echo ""

# Navigate to the project directory
cd "$(dirname "$0")/ReferenceDataApi"

# Check if project file exists
if [ ! -f "ReferenceDataApi.csproj" ]; then
    echo "ERROR: Project file not found. Make sure you're in the correct directory."
    exit 1
fi

# Create necessary directories
echo "Creating necessary directories..."
mkdir -p logs uploads archive temp

# Restore dependencies
echo "Restoring NuGet packages..."
dotnet restore

# Build the project
echo "Building the project..."
dotnet build --configuration Release

# Check if build succeeded
if [ $? -ne 0 ]; then
    echo "ERROR: Build failed. Please check the errors above."
    exit 1
fi

echo ""
echo "Build completed successfully!"
echo ""
echo "Starting the API server..."
echo "API will be available at: http://localhost:5000"
echo "Swagger UI will be available at: http://localhost:5000/swagger"
echo ""
echo "Press Ctrl+C to stop the server"
echo ""

# Run the application (URLs configured in appsettings.json)
dotnet run --configuration Release
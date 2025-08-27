using System;
using ReferenceDataApi.Infrastructure;
using ReferenceDataApi.Services;

namespace DatabaseConnectionTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Testing Database Connection via DatabaseManager");
            Console.WriteLine("=============================================");
            
            try
            {
                // Create a simple logger
                var logger = new SystemLogger();
                
                // Create DatabaseManager instance
                var dbManager = new DatabaseManager(logger);
                
                Console.WriteLine("DatabaseManager created successfully.");
                Console.WriteLine("Connection string configured from appsettings.json");
                
                // Test the connection
                Console.WriteLine("\nTesting database connection...");
                bool connectionSuccess = dbManager.TestConnection();
                
                if (connectionSuccess)
                {
                    Console.WriteLine("✅ SUCCESS: Database connection test passed!");
                    
                    // Try to get available schemas
                    Console.WriteLine("\nRetrieving available schemas...");
                    try
                    {
                        var schemas = dbManager.GetAvailableSchemas();
                        Console.WriteLine($"Found {schemas.Count} schemas:");
                        foreach (var schema in schemas)
                        {
                            Console.WriteLine($"  - {schema}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"❌ Error retrieving schemas: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("❌ FAILED: Database connection test failed!");
                    Console.WriteLine("Check the connection string in appsettings.json");
                    Console.WriteLine("Ensure SQL Server is running and accessible");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ ERROR: Failed to initialize DatabaseManager: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
            
            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }
    }
}

using System;
using System.Data.SqlClient;

class DatabaseTest
{
    static void Main(string[] args)
    {
        Console.WriteLine("Database Connection Test");
        Console.WriteLine("========================");
        
        // Test different connection string variations
        string[] connectionStrings = {
            // Windows Authentication tests (recommended)
            "Server=.\\SQL2ETL;Database=master;Integrated Security=true;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=localhost\\SQL2ETL;Database=master;Integrated Security=true;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=LIN9400F\\SQL2ETL;Database=master;Integrated Security=true;TrustServerCertificate=true;Connection Timeout=5;",
            // SQL Authentication tests (if configured)
            "Server=.\\SQL2ETL;Database=master;Integrated Security=false;User ID=sa;Password=YourSAPassword;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=LIN9400F\\SQL2ETL;Database=test;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=localhost\\SQL2ETL;Database=test;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=localhost\\SQL2ETL;Database=master;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=.\\SQL2ETL;Database=test;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=5;",
            "Server=127.0.0.1\\SQL2ETL;Database=test;Integrated Security=false;User ID=tester;Password=121@abc!;TrustServerCertificate=true;Connection Timeout=5;"
        };
        
        for (int i = 0; i < connectionStrings.Length; i++)
        {
            Console.WriteLine($"\nTest #{i + 1}:");
            Console.WriteLine($"Connection: {connectionStrings[i]}");
            
            try
            {
                using (var connection = new SqlConnection(connectionStrings[i]))
                {
                    Console.WriteLine("Attempting to open connection...");
                    connection.Open();
                    
                    Console.WriteLine("✅ SUCCESS: Connection opened successfully!");
                    Console.WriteLine($"Database: {connection.Database}");
                    Console.WriteLine($"Server Version: {connection.ServerVersion}");
                    Console.WriteLine($"Data Source: {connection.DataSource}");
                    
                    // Try a simple query
                    using (var command = new SqlCommand("SELECT @@VERSION", connection))
                    {
                        var result = command.ExecuteScalar();
                        Console.WriteLine($"SQL Server Version: {result}");
                    }
                    
                    connection.Close();
                    Console.WriteLine("Connection closed successfully.");
                    break; // Exit loop on first success
                }
            }
            catch (SqlException sqlEx)
            {
                Console.WriteLine($"❌ SQL ERROR: {sqlEx.Message}");
                Console.WriteLine($"Error Number: {sqlEx.Number}");
                Console.WriteLine($"Severity: {sqlEx.Class}");
                Console.WriteLine($"State: {sqlEx.State}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ GENERAL ERROR: {ex.Message}");
                Console.WriteLine($"Exception Type: {ex.GetType().Name}");
            }
        }
        
        Console.WriteLine("\nDatabase connection test completed.");
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
}
using System;
using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using ReferenceDataApi.Infrastructure;
using ReferenceDataApi.Services;

namespace ReferenceDataApi
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Configure server URLs from configuration with environment variable expansion
            var host = ExpandEnvironmentVariables(builder.Configuration["ServerSettings:Host"]) ?? "localhost";
            var httpPortStr = ExpandEnvironmentVariables(builder.Configuration["ServerSettings:HttpPort"]) ?? "8000";
            var httpsPortStr = ExpandEnvironmentVariables(builder.Configuration["ServerSettings:HttpsPort"]) ?? "8001";
            
            var httpPort = int.Parse(httpPortStr);
            var httpsPort = int.Parse(httpsPortStr);
            
            var httpUrl = string.Format("http://{0}:{1}", host, httpPort);
            var httpsUrl = string.Format("https://{0}:{1}", host, httpsPort);
            
            builder.WebHost.UseUrls(httpUrl, httpsUrl);

            // Add services to the container - .NET Framework 4.5 compatible way
            ConfigureServices(builder.Services, builder.Configuration);

            var app = builder.Build();

            // Configure the HTTP request pipeline - .NET Framework 4.5 compatible way
            Configure(app, app.Environment);

            app.Run();
        }

        private static string ExpandEnvironmentVariables(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            try
            {
                // Expand environment variables in format ${VAR_NAME:default_value}
                var result = System.Text.RegularExpressions.Regex.Replace(input, @"\$\{([^}:]+)(?::([^}]*))?\}", match =>
                {
                    var varName = match.Groups[1].Value;
                    var defaultValue = match.Groups.Count > 2 ? match.Groups[2].Value : "";
                    var envValue = Environment.GetEnvironmentVariable(varName);
                    return envValue ?? defaultValue;
                });

                return result;
            }
            catch (Exception)
            {
                return input; // Return original string if expansion fails
            }
        }

        private static void ConfigureServices(IServiceCollection services, IConfiguration configuration)
        {
            // Add controllers
            services.AddControllers();

            // Add CORS
            services.AddCors(options =>
            {
                options.AddDefaultPolicy(builder =>
                {
                    builder.AllowAnyOrigin()
                           .AllowAnyMethod() 
                           .AllowAnyHeader();
                });
            });

            // Add Swagger
            services.AddSwaggerGen();

            // Add custom services - .NET Framework 4.5 compatible dependency injection
            services.AddSingleton<IDatabaseManager, DatabaseManager>();
            services.AddSingleton<IFileHandler, FileHandler>();
            services.AddSingleton<ILogger, SystemLogger>();
            services.AddSingleton<IProgressTracker, ProgressTracker>();
            services.AddSingleton<ICsvDetector, CsvDetector>();
            services.AddSingleton<IDataIngestion, DataIngestion>();
        }

        private static void Configure(WebApplication app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseCors();
            
            app.UseRouting();
            
            app.MapControllers();

            // Database initialization removed - assume tables and procedures already exist
        }
    }
}
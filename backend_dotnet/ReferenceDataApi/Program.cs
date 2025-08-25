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

            // Add services to the container - .NET Framework 4.5 compatible way
            ConfigureServices(builder.Services, builder.Configuration);

            var app = builder.Build();

            // Configure the HTTP request pipeline - .NET Framework 4.5 compatible way
            Configure(app, app.Environment);

            app.Run();
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
        }
    }
}
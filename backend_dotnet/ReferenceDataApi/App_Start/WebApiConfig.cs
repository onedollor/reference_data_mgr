using System.Web.Http;
using System.Web.Http.Cors;

namespace ReferenceDataApi
{
    public static class WebApiConfig
    {
        public static void Register(HttpConfiguration config)
        {
            // Enable CORS for all origins, headers, and methods
            var cors = new EnableCorsAttribute("*", "*", "*");
            config.EnableCors(cors);

            // Web API configuration and services
            config.Formatters.JsonFormatter.SerializerSettings.Formatting = Newtonsoft.Json.Formatting.Indented;
            config.Formatters.JsonFormatter.SerializerSettings.ContractResolver = new Newtonsoft.Json.Serialization.CamelCasePropertyNamesContractResolver();

            // Web API routes
            config.Routes.MapHttpRoute(
                name: "Health",
                routeTemplate: "api/{controller}/health",
                defaults: new { action = "GetHealth" }
            );

            config.Routes.MapHttpRoute(
                name: "Config",
                routeTemplate: "api/{controller}/config",
                defaults: new { action = "GetConfig" }
            );

            config.Routes.MapHttpRoute(
                name: "Schemas",
                routeTemplate: "api/{controller}/schemas",
                defaults: new { action = "GetSchemas" }
            );

            config.Routes.MapHttpRoute(
                name: "Tables",
                routeTemplate: "api/{controller}/tables/{schemaName}",
                defaults: new { action = "GetTables" }
            );

            config.Routes.MapHttpRoute(
                name: "Columns",
                routeTemplate: "api/{controller}/columns/{schemaName}/{tableName}",
                defaults: new { action = "GetColumns" }
            );

            config.Routes.MapHttpRoute(
                name: "Test",
                routeTemplate: "api/{controller}/test",
                defaults: new { action = "GetTest" }
            );

            config.Routes.MapHttpRoute(
                name: "DefaultApi",
                routeTemplate: "api/{controller}/{id}",
                defaults: new { id = RouteParameter.Optional }
            );

            // Remove XML formatter to only return JSON
            config.Formatters.Remove(config.Formatters.XmlFormatter);
        }
    }
}
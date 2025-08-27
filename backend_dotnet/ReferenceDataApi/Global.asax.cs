using System;
using System.Web;
using System.Web.Http;

namespace ReferenceDataApi
{
    public class Global : HttpApplication
    {
        protected void Application_Start()
        {
            GlobalConfiguration.Configure(WebApiConfig.Register);
        }

        protected void Application_Error()
        {
            var exception = Server.GetLastError();
            // Log the exception here if needed
        }

        protected void Application_End()
        {
            // Application cleanup code here
        }
    }
}
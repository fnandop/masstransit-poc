using CommonLegacy;
using MassTransit;
using MassTransit.Util;
using System;
using System.Configuration;

namespace WebApplicationLegacy
{
    public class Global : System.Web.HttpApplication
    {

        static BusHandle _busHandle;
        protected void Application_Start(object sender, EventArgs e)
        {
            _busHandle = TaskUtil.Await(() => MassTransitBusManager.StartAsync(IBusControlFactory.CreateUsing(ConfigurationManager.AppSettings["TransportName"])));
        }

        protected void Session_Start(object sender, EventArgs e)
        {

        }

        protected void Application_BeginRequest(object sender, EventArgs e)
        {

        }

        protected void Application_AuthenticateRequest(object sender, EventArgs e)
        {

        }

        protected void Application_Error(object sender, EventArgs e)
        {

        }

        protected void Session_End(object sender, EventArgs e)
        {

        }

        protected void Application_End(object sender, EventArgs e)
        {
            _busHandle?.Stop(TimeSpan.FromSeconds(30));

        }
    }
}
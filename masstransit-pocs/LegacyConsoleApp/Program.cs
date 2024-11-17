using MassTransit;
using NLog;
using Topshelf;

namespace ConsumerServiceApp
{
    public class Program
    {


        static int Main(string[] args)
        {
            
            var loggerFactory = new NLog.Extensions.Logging.NLogLoggerFactory();
            
            LogContext.ConfigureCurrentLogContext(loggerFactory);

            return (int)HostFactory.Run(x =>
            {
                x.Service<ConsumerService>();
                x.SetDescription("Command and Events Consumer Service");
                x.SetDisplayName("ConsumerService");
                x.SetServiceName("ConsumerService");



            });
        }
    }
}

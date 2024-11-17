using CommonLegacy;
using LegacyConsoleApp.Consumers;
using MassTransit;
using MassTransit.Util;
using System;
using System.Configuration;
using Topshelf;

namespace ConsumerServiceApp
{
    internal class ConsumerService : ServiceControl
    {
        private static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        public bool Start(HostControl hostControl)
        {

            _logger.Info("Starting bus...");
            TaskUtil.Await(() => MassTransitBusManager.StartAsync(IBusControlFactory.CreateUsing(
                                                                                                ConfigurationManager.AppSettings["TransportName"],
                                                                                                cfg => cfg.ReceiveEndpoint("CommandsQueue", e =>
                                                                                                 {
                                                                                                     e.Consumer<ApproveSomethingCommandConsumer>();
                                                                                                     e.Consumer<SomethingApprovedEventConsumer>();
                                                                                                     e.Consumer<UnreliableServiceCallSomethingApprovedEventConsumer>(c =>
                                                                                                     {
                                                                                                         c.UseMessageRetry(r =>
                                                                                                         {
                                                                                                             r.Interval(5, TimeSpan.FromSeconds(5));
                                                                                                         });
                                                                                                     });
                                                                                                     e.PrefetchCount = 1;
                                                                                                 })
                                                                                                )));
            _logger.Info("Bus started...");
            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            _logger.Info("Stopping bus...");
            TaskUtil.Await(() => MassTransitBusManager.StopAsync());
            _logger.Info("Bus stoped...");
            return true;
        }
    }
}


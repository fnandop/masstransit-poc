using CommonLegacy;
using MassTransit;
using MessageContracts;
using System;
using System.Threading.Tasks;

namespace LegacyConsoleApp.Consumers
{
    internal class ApproveSomethingCommandConsumer : IConsumer<ApproveSomethingCommand>
    {
        private static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        public async Task Consume(ConsumeContext<ApproveSomethingCommand> context)
        {
            _logger.Info($"Reveived {nameof(ApproveSomethingCommand)}");
            await MassTransitBusManager.Bus.Publish(new SomethingApprovedEvent { SomethingId = context.Message.SomethingId });
        }
    }
}

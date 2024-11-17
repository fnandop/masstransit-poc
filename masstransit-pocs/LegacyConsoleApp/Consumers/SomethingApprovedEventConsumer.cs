using MassTransit;
using MessageContracts;
using System.Threading.Tasks;

namespace LegacyConsoleApp.Consumers
{
    internal class SomethingApprovedEventConsumer : IConsumer<SomethingApprovedEvent>
    {
        private static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();

        public Task Consume(ConsumeContext<SomethingApprovedEvent> context)
        {
            _logger.Info($"Handling {nameof(SomethingApprovedEvent)}: " +
                         $"SomethingId = {context.Message.SomethingId} " );

            // Additional business logic for handling the event can go here.

            _logger.Info($"Successfully handled {nameof(SomethingApprovedEvent)} with SomethingId = {context.Message.SomethingId}.");
            return Task.CompletedTask;
        }
    }
}

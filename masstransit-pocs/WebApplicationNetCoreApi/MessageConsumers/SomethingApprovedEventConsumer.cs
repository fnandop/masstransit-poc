using MassTransit;
using MessageContracts;

namespace WebApplicationNetCoreApi.MessageConsumers
{
    internal class SomethingApprovedEventConsumer : IConsumer<SomethingApprovedEvent>
    {
        ILogger<SomethingApprovedEventConsumer> _logger;

        public SomethingApprovedEventConsumer(ILogger<SomethingApprovedEventConsumer> logger) => _logger = logger;

        public Task Consume(ConsumeContext<SomethingApprovedEvent> context)
        {
            _logger.LogInformation($"{nameof(SomethingApprovedEvent)} Received by SomethingApprovedEvent");

            // Simulating WebSocket notification to SPA client
            _logger.LogInformation("Pretending to notify SPA clients via WebSocket: 'Something has been approved!'");

            return Task.CompletedTask;
        }
    }
}

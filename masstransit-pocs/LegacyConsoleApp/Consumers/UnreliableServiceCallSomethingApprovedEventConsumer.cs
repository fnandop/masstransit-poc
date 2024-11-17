using MassTransit;
using MessageContracts;
using System;
using System.Threading.Tasks;

namespace LegacyConsoleApp.Consumers
{
    internal class UnreliableServiceCallSomethingApprovedEventConsumer : IConsumer<SomethingApprovedEvent>
    {
        private static readonly NLog.Logger _logger = NLog.LogManager.GetCurrentClassLogger();
        private static readonly Random _random = new Random();

        public Task Consume(ConsumeContext<SomethingApprovedEvent> context)
        {
            _logger.Info($"Received message: {context.Message}");
            _logger.Info($"Received {nameof(SomethingApprovedEvent)}");

            // Print retry-related information
            var retryCount = context.GetRetryCount(); // Get the current retry count
            var redeliveryCount = context.GetRedeliveryCount(); // Get the redelivery count

            if (retryCount > 0 || redeliveryCount > 0)
            {
                _logger.Info($"Retry attempt {retryCount} - Redelivery attempt {redeliveryCount}");
            }

            // Simulate success or failure
            if (ShouldFail())
            {
                _logger.Warn($"Simulated failure in {nameof(UnreliableServiceCallSomethingApprovedEventConsumer)}.");
                throw new Exception("Simulated exception to mimic unreliable service.");
            }

            _logger.Info($"Successfully processed {nameof(SomethingApprovedEvent)}.");
            return Task.CompletedTask;
        }

        private bool ShouldFail()
        {
            // Randomly fail 50% of the time
            return _random.Next(0, 2) == 0;
        }
    }

}

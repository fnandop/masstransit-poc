using MassTransit;
using MassTransit.Transports;
using MessageContracts;
using Microsoft.AspNetCore.Mvc;

namespace WebApplicationNetCoreApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class SomethingsController : ControllerBase
    {

        private readonly ISendEndpointProvider _sendEndpointProvider;
        private readonly ILogger<SomethingsController> _logger;

        public SomethingsController(ILogger<SomethingsController> logger, ISendEndpointProvider sendEndpointProvider)
        {
            _logger = logger;
            _sendEndpointProvider = sendEndpointProvider;
        }

        [HttpPost(Name = "approve")]
        public async Task<IActionResult> Approve()
        {
            var endpoint = await _sendEndpointProvider.GetSendEndpoint(new Uri("queue:CommandsQueue"));
            await endpoint.Send(new ApproveSomethingCommand { SomethingId = 99 });
            return Ok($"{nameof(ApproveSomethingCommand)} command sent successfully!");
        }
    }
}
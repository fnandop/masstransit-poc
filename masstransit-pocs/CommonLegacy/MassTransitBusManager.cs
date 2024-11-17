using MassTransit;
using System.Threading;
using System.Threading.Tasks;
using MassTransit.Util;

namespace CommonLegacy
{
    public sealed class MassTransitBusManager
    {
        static IBusControl _busControl;
        public static IBus Bus => _busControl;

        private static readonly object _lock = new object();
        public static Task<BusHandle> StartAsync(IBusControl busControl, CancellationToken cancellationToken = default)
        {
            if (_busControl == null)
            {
                lock (_lock)
                {
                    if (_busControl == null)
                    {
                        _busControl = busControl;
                    }
                }
            }
            return _busControl.StartAsync(cancellationToken);

        }
        public static Task StopAsync(CancellationToken cancellationToken = default) { return _busControl.StopAsync(cancellationToken); }

    }


}

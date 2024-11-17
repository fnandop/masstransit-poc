using CommonLegacy;
using MessageContracts;
using System;
using System.Threading.Tasks;
using System.Web.Services;

namespace WebApplicationLegacy
{
    /// <summary>
    /// Summary description for LegacyWebService
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    // [System.Web.Script.Services.ScriptService]
    public class LegacyWebService : System.Web.Services.WebService
    {

        [WebMethod]
        public string ApproveSomething(int SomethingId)
        {
            Task.Run(async () =>
            {
                var sendEndPoint = await MassTransitBusManager.Bus.GetSendEndpoint(new Uri("queue:CommandsQueue"));
                await sendEndPoint.Send(new ApproveSomethingCommand { SomethingId = 99 });
            }).Wait(); // Wait to ensure the task completes before the method returns.
            return string.Empty;
        }
    }
}

namespace MessageContracts
{
    public interface IntegrationEvent { }
    public interface IntegrationCommand { }
    public class SomethingApprovedEvent : IntegrationEvent
    {
        public int SomethingId { get; set; }
    }

    public class ApproveSomethingCommand : IntegrationCommand
    {
        public int SomethingId { get; set; }
    }


    public class DoSomethingInUnreliableServiceCommand : IntegrationCommand
    {
        public int SomethingId { get; set; }
    }
}

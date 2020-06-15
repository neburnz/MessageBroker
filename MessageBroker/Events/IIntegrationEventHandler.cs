using System.Threading.Tasks;

namespace MessageBroker.Events
{
    public interface IIntegrationEventHandler<in T> : IIntegrationEventHandler
    where T : IntegrationEvent
    {
        Task Handle(T @event);
    }

    public interface IIntegrationEventHandler
    {

    }
}
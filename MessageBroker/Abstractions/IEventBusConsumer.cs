using System;
using MessageBroker.Events;

namespace MessageBroker.Abstractions
{
    public interface IEventBusConsumer
    {
        void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;
        void Unsubscribe<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent;
        bool HasMessages();
    }
}
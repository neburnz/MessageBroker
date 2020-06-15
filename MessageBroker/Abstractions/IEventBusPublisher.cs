using System;
using MessageBroker.Events;

namespace MessageBroker.Abstractions
{
    public interface IEventBusPublisher
    {
        void Publish(IntegrationEvent @event, byte priority = 0);
    }
}
using System;
using System.Runtime.Serialization;
using MessageBroker.Events;

namespace MessageBroker.Tests.Events
{
    [Serializable]
    public sealed class TestIntegrationEvent: IntegrationEvent
    {
        public TestIntegrationEvent()
        {
        }
        private TestIntegrationEvent(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
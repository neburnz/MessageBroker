using System;
using System.Threading.Tasks;
using MessageBroker.Events;

namespace MessageBroker.Tests.Events
{
    public class TestIntegrationEventHandler: IIntegrationEventHandler<TestIntegrationEvent>
    {
        public bool Handled { get; private set; }
        public TestIntegrationEventHandler()
        {
            Handled = false;
        }
        public async Task Handle(TestIntegrationEvent @event)
        {
            await Task.Run(() => SetHandled());
        }

        private void SetHandled()
        {
            Handled = true;
        }
    }
}
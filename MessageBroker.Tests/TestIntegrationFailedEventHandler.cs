using System;
using System.Threading.Tasks;
using NLog;
using MessageBroker.Events;

namespace MessageBroker.Tests.Events
{
    public class TestIntegrationFailedEventHandler: IIntegrationEventHandler<TestIntegrationEvent>
    {
        private readonly Logger logger;
        public TestIntegrationFailedEventHandler(Logger logger)
        {
            this.logger = logger;
        }
        public Task Handle(TestIntegrationEvent @event)
        {
            return Task.Run(() => 
            {
                logger.Info($"Iniciando el método Handle"); 
                Execute();
            });
        }
        private void Execute()
        {
            logger.Warn($"Se lanza la excepción dentro del método Execute");
            throw new NotImplementedException();
        }
    }
}
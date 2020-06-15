using System;
using System.Net.Sockets;
using System.Text;
using Newtonsoft.Json;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using NLog;
using MessageBroker.Abstractions;
using MessageBroker.Events;

namespace MessageBroker.RabbitMQ
{
    public class EventBusPublisher : IEventBusPublisher
    {
        const string BROKER_NAME = "app_event_bus";
        private readonly IRabbitMQPersistentConnection _persistentConnection;
        private readonly int _retryCount;

        public EventBusPublisher(IRabbitMQPersistentConnection persistentConnection, int retryCount = 5)
        {
            logger.Info("Se inicia la ejecución del constructor");
            logger.Debug("Argumentos del constructor: persistentConnection - {}, retryCount - {}",
                persistentConnection, retryCount);

            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _retryCount = retryCount;
        }
        private static readonly Logger logger = LogManager.GetLogger(typeof(EventBusPublisher).FullName);
        public void Publish(IntegrationEvent @event, byte priority = 0)
        {
            logger.Info("Se inicia la ejecución del método Publish");
            logger.Debug("Argumentos del método Publish: event - {}, priority - {}", @event, priority);

            if (!_persistentConnection.IsConnected)
            {
                logger.Trace("No se ha establecido la conexión al Message Broker");
                _persistentConnection.TryConnect();
            }

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    logger.Warn("No se ha podido establecer la conexión al Message Broker");
                    logger.Debug("Argumentos del action onRetry: Exception - {}, TimeSpan - {}",
                            ex, time);
                });

            logger.Trace("Se procede a crear el Channel");
            using (var channel = _persistentConnection.CreateModel())
            {
                logger.Debug("Valor de la variable channel - {}", channel);

                logger.Trace("Se obtiene el nombre del evento y se almacena en la variable eventName");
                var eventName = @event.GetType()
                    .Name;
                logger.Debug("Valor de la variable eventName - {}", eventName);

                logger.Trace("Se declara el Exchange en el Message Broker");
                logger.Debug("Argumentos de channel.ExchangeDeclare: exchange - {}, type - {}, durable - {}",
                    BROKER_NAME, "direct", true);
                channel.ExchangeDeclare(exchange: BROKER_NAME,
                                    type: "direct", durable: true);

                logger.Trace("Se obtiene la instancia del IntegrationEvent");
                var message = JsonConvert.SerializeObject(@event);
                logger.Debug("Valor de la variable message - {}", message);

                logger.Trace("Se obtiene el payload del IntegrationEvent");
                var body = Encoding.UTF8.GetBytes(message);
                logger.Debug("Valor de la variable body - {}", body);

                logger.Trace("Se define la política para la publicación del evento");
                policy.Execute(() =>
                {
                    logger.Trace("Se inicia la creación de las propiedades del mensaje");
                    var properties = channel.CreateBasicProperties();
                    logger.Debug("Valor de la variable properties - {}", properties);

                    logger.Trace("Se establece el DeliveryMode");
                    properties.DeliveryMode = 2; // persistent
                    logger.Debug("Valor del properties.DeliveryMode - {}", properties.DeliveryMode);

                    logger.Trace("Se establece el Priority");
                    properties.Priority = priority;
                    logger.Debug("Valor del properties.Priority - {}", properties.Priority);

                    logger.Trace("Se realiza la publicación del mensaje en el Message Broker");
                    logger.Debug("Argumentos de channel.BasicPublish: exchange - {}, routingKey - {}, mandatory - {}, basicProperties - {}, body - {}",
                        BROKER_NAME, eventName, true, properties, body);
                    channel.BasicPublish(exchange: BROKER_NAME,
                                     routingKey: eventName,
                                     mandatory: true,
                                     basicProperties: properties,
                                     body: body);
                });
            }
        }
    }
}
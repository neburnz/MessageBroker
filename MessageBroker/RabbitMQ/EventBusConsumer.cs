using System;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Autofac;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using NLog;
using MessageBroker.Abstractions;
using MessageBroker.Events;

namespace MessageBroker.RabbitMQ
{
    public sealed class EventBusConsumer : IEventBusConsumer, IDisposable
    {
        private static readonly Logger logger = LogManager.GetLogger(typeof(EventBusConsumer).FullName);
        const string BROKER_NAME = "event_bus";
        private readonly IRabbitMQPersistentConnection _persistentConnection;
        private readonly IEventBusSubscriptionsManager _subsManager;
        private readonly ILifetimeScope _autofac;
        private readonly string AUTOFAC_SCOPE_NAME = "event_bus";
        private IModel _consumerChannel;
        private string _queueName;
        private readonly uint _prefetchSize;
        private readonly ushort _prefetchCount;
        public EventBusConsumer(IRabbitMQPersistentConnection persistentConnection, ILifetimeScope autofac,
            IEventBusSubscriptionsManager subsManager, string queueName = null, uint prefetchSize = 0, ushort prefetchCount = 1,
            bool consumirMensajes = true)
        {
            logger.Info("Se inicia la ejecución del constructor");
            logger.Debug("Argumentos del constructor: persistentConnection - {}, autofac - {}, subsManager - {}, queueName - {}, prefetchSize - {}, prefetchCount - {}",
                persistentConnection, autofac, subsManager, queueName, prefetchSize, prefetchCount);

            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _subsManager = subsManager ?? new InMemoryEventBusSubscriptionsManager();
            _queueName = queueName;
            _prefetchSize = prefetchSize;
            _prefetchCount = prefetchCount;
            _consumerChannel = CreateConsumerChannel(consumirMensajes);
            _autofac = autofac;
            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }
        public void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            logger.Info("Se inicia la ejecución del método Subscribe");
            var eventName = _subsManager.GetEventKey<T>();
            logger.Debug("Valor de la variable eventName - {}", eventName);

            DoInternalSubscription(eventName);
            _subsManager.AddSubscription<T, TH>();
        }
        public void Unsubscribe<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent
        {
            logger.Info("Se inicia la ejecución del método Unsubscribe");
            _subsManager.RemoveSubscription<T, TH>();
        }
        public void Dispose()
        {
            logger.Info("Se inicia la ejecución del método Dispose");

            if (_consumerChannel != null)
            {
                logger.Trace("Se realiza el Dispose del Consumer Channel");
                logger.Debug("Valor del campo _consumerChannel - {}", _consumerChannel);
                _consumerChannel.Dispose();
            }

            logger.Trace("Se realiza la limpieza del Subscription Manager");
            _subsManager.Clear();
        }
        private void DoInternalSubscription(string eventName)
        {
            logger.Info("Se inicia la ejecución del método DoInternalSubscription");
            logger.Debug("Argumentos del método DoInternalSubscription: eventName - {}", eventName);

            logger.Trace("Se verifica la existencia de suscripciones al evento {}", eventName);
            var containsKey = _subsManager.HasSubscriptionsForEvent(eventName);
            logger.Debug("Valor de la variable containsKey - {}", containsKey);

            if (!containsKey)
            {
                logger.Trace("No se ha encontrado suscripciones al evento: {}", eventName);

                if (!_persistentConnection.IsConnected)
                {
                    logger.Trace("No se ha establecido la conexión al Message Broker");
                    _persistentConnection.TryConnect();
                }

                logger.Trace("Se ha establecido la conexión al Message Broker, se procede a crear el Channel");
                using (var channel = _persistentConnection.CreateModel())
                {
                    logger.Debug("Valor de la variable channel - {}", channel);

                    logger.Trace("Se inicia la creación del Binding hacia Queue del Message Broker");
                    logger.Debug("Argumentos de channel.QueueBind: queue - {}, exchange - {}, routingKey - {}",
                        _queueName, BROKER_NAME, eventName);
                    channel.QueueBind(queue: _queueName,
                                      exchange: BROKER_NAME,
                                      routingKey: eventName);
                }
            }
        }
        private IModel CreateConsumerChannel(bool consumir)
        {
            logger.Info("Se inicia el método CreateConsumerChannel");

            if (_persistentConnection == null)
                logger.Warn("La conexión al Message Broker es una instancia nula");

            if (!_persistentConnection.IsConnected)
            {
                logger.Trace("La conexión al Message Broker está cerrada, se intenta conectar");
                _persistentConnection.TryConnect();
            }

            logger.Trace("Se creal el channel hacia el Message Broker");
            var channel = _persistentConnection.CreateModel();
            if (channel == null)
                logger.Warn("El channel hacia el Message Broker es una instancia nula");
            logger.Debug("Valor de la variable channel - {}", channel);

            logger.Trace("Se declara el exchange hacia el Message Broker");
            logger.Debug("Argumentos de channel.ExchangeDeclare: exchange - {}, type - {}, durable - {}",
                BROKER_NAME, "direct", true);
            channel.ExchangeDeclare(exchange: BROKER_NAME,
                                 type: "direct", durable: true);

            logger.Trace("Se declara el queue en el Message Broker");
            logger.Debug("Argumentos de channel.QueueDeclare: queue - {}, durable - {}, exclusive - {}, autoDelete - {}, arguments - {}",
                _queueName, true, false, false, null);
            channel.QueueDeclare(queue: _queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            logger.Trace("Se obtiene la instancia del Consumer");
            var consumer = new EventingBasicConsumer(channel);
            logger.Debug("Valor de la variable consumer - {}", consumer);
            consumer.Received += async (model, ea) =>
            {
                logger.Info("Se inicia el event handler Received del consumer");
                logger.Debug("Argumentos del event handler Received: sender - {}, eventArgs - {}",
                    model, ea);

                logger.Trace("Se obtiene el RoutingKey y el Body del eventArgs");
                var eventName = ea.RoutingKey;
                var message = Encoding.UTF8.GetString(ea.Body);
                logger.Debug("Valor de la variable eventName - {}, message - {}",
                    eventName, message);

                try
                {
                    if (!_subsManager.HasSubscriptionsForEvent(eventName))
                    {
                        logger.Trace("Se realiza la notificaión de Nack del mensaje");
                        logger.Debug("Argumentos de channel.BasicNack: deliveryTag - {}, multiple - {}, requeue - {}", ea.DeliveryTag, false, true);
                        channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
                        return;
                    }

                    logger.Trace("Se realiza el procesamiento del evento");
                    // Event is processed
                    await ProcessEvent(eventName, message);

                    // The message is acknowledged and removed from the queue
                    logger.Trace("Se realiza la notificación de Ack del mensaje");
                    logger.Debug("Argumentos de channel.BasicAck: deliveryTag - {}, multiple - {}",
                        ea.DeliveryTag, false);
                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                }
                catch (Exception exception)
                {
                    // The exception is thrown
                    logger.Error(exception, $"Ha ocurrido una excepción al tratar de manejar el evento: {eventName}");
                    // The message is rejected and returned to queue
                    logger.Trace("Se realiza la notificaión de Nack del mensaje");
                    logger.Debug("Argumentos de channel.BasicNack: deliveryTag - {}, multiple - {}, requeue - {}",
                        ea.DeliveryTag, false, true);
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: true);
                }
            };

            logger.Trace("Se configura el QOS del Channel");
            logger.Debug("Argumentos de channel.BasicQos: prefetchSize - {}, prefetchCount - {}, global - {}",
                _prefetchSize, _prefetchCount, false);
            channel.BasicQos(prefetchSize: _prefetchSize, prefetchCount: _prefetchCount, global: false);

            if (consumir)
            {
                logger.Trace("Se inicia el consumo del Channel");
                logger.Debug("Argumentos de channel.BasicConsume: queue - {}, autoAck - {}, consumer - {}",
                    _queueName, false, consumer);
                channel.BasicConsume(queue: _queueName,
                                     autoAck: false,
                                     consumer: consumer);
            }

            channel.CallbackException += (sender, ea) =>
            {
                logger.Info("Se inicia la ejecución del event handler CallbackException del channel");
                logger.Debug("Argumentos del event handler CallbackException: sender - {}, eventArgs - {}",
                    sender, ea);

                logger.Trace("Se realiza el Dispose del Consumer");
                _consumerChannel.Dispose();

                logger.Trace("Se crea una nueva instancia del Consumer");
                _consumerChannel = CreateConsumerChannel(consumir);
            };

            return channel;
        }
        private async Task ProcessEvent(string eventName, string message)
        {
            logger.Info("Se inicia la ejecución de la tarea ProcessEvent");
            logger.Debug("Argumentos de la tarea ProcessEvent: eventName - {}, message - {}",
                eventName, message);

            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                logger.Trace("Se ha encontrado la suscripción del evento: {}", eventName);
                using (var scope = _autofac.BeginLifetimeScope(AUTOFAC_SCOPE_NAME))
                {
                    logger.Trace("Se crea el ámbito del contenedor con el identificador: {}", AUTOFAC_SCOPE_NAME);

                    logger.Trace("Se intentan obtener los handler registrados para el evento: {}", eventName);
                    var subscriptions = _subsManager.GetHandlersForEvent(eventName);
                    if (subscriptions == null || !subscriptions.Any())
                        logger.Warn($"No se han registrado handlers para el evento: {eventName}");

                    logger.Trace("Se itera la lista de suscripciones para procesar el evento: {}", eventName);
                    foreach (var subscription in subscriptions ?? Enumerable.Empty<InMemoryEventBusSubscriptionsManager.SubscriptionInfo>())
                    {
                        var eventType = _subsManager.GetEventTypeByName(eventName);
                        logger.Debug("Se obtiene el eventType de Subscription Manager: {}", eventType);
                        var integrationEvent = JsonConvert.DeserializeObject(message, eventType);
                        logger.Debug("Se obtiene el integrationEvent a través de la deserialización del message: {}", integrationEvent);
                        var handler = scope.ResolveOptional(subscription.HandlerType);
                        logger.Debug("Se obtiene la instancia del handler del contenedor: {}", handler);
                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        logger.Debug("Se obtiene el concreteType del IntegrationEventHandler: {}", concreteType);

                        logger.Trace("Se realiza la invocación del método Handle del IntegrationEventHandler");
                        await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
                    }
                }
            }
        }
        private void SubsManager_OnEventRemoved(object sender, string eventName)
        {
            logger.Info("Se inicia la ejecución del event handler SubsManager_OnEventRemoved");
            logger.Debug("Argumentos del event handler SubsManager_OnEventRemoved: sender - {}, eventArgs - {}",
                sender, eventName);

            if (_persistentConnection == null)
                logger.Warn("La instancia del campo _persistentConnection es nula");

            if (!_persistentConnection.IsConnected)
            {
                logger.Trace("La conexión hacia el Message Broker no ha sido establecida");
                _persistentConnection.TryConnect();
            }

            logger.Trace("Se procede a crear el Channel");
            using (var channel = _persistentConnection.CreateModel())
            {
                logger.Debug("Valor de la variable channel - {}", channel);

                logger.Trace("Se intenta remover el Binding hacie el Queue del Message Broker");
                logger.Debug("Argumentos de channel.QueueUnbind: queue - {}, exchange - {}, routingKey - {}",
                    _queueName, BROKER_NAME, eventName);
                channel.QueueUnbind(queue: _queueName,
                    exchange: BROKER_NAME,
                    routingKey: eventName);

                if (_subsManager.IsEmpty)
                {
                    logger.Trace("La colección de handlers del SubscrptionManager está vacía");
                    logger.Debug("Se limpia el valor de _queueName");
                    _queueName = string.Empty;
                    logger.Debug("Se cierra la sesión del _consumerChannel");
                    _consumerChannel.Close();
                }
            }
        }

        public bool HasMessages()
        {
            logger.Info("Se inicia la ejecución del método HasMessages");

            if (_persistentConnection == null)
                logger.Warn("La instancia del campo _persistentConnection es nula");

            if (!_persistentConnection.IsConnected)
            {
                logger.Trace("La conexión hacia el Message Broker no ha sido establecida");
                _persistentConnection.TryConnect();
            }

            logger.Trace("Se procede a crear el Channel");
            using (var channel = _persistentConnection.CreateModel())
            {
                logger.Debug("Valor de la variable channel - {}", channel);
                logger.Debug("Argumentos de channel.MessageCount: queue - {}", _queueName);
                var messageCount = channel.MessageCount(_queueName);

                logger.Debug("Valor de la variable messageCount - {}", messageCount);
                return messageCount > 0u;
            }
        }
    }
}
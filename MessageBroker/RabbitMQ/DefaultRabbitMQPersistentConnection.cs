using System;
using System.IO;
using System.Net.Sockets;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using NLog;

namespace MessageBroker.RabbitMQ
{
    public sealed class DefaultRabbitMQPersistentConnection : IRabbitMQPersistentConnection
    {
        private readonly IConnectionFactory _connectionFactory;

        private readonly int _retryCount;
        IConnection _connection;
        bool _disposed;
        private readonly object sync_root = new object();

        public DefaultRabbitMQPersistentConnection(IConnectionFactory connectionFactory, int retryCount = 5)
        {
            logger.Info("Se inicia la ejecución del constructor");
            logger.Debug("Argumentos del constructor: connectionFactory - {}, retryCount - {}",
                connectionFactory, retryCount);

            if (connectionFactory == null)
                logger.Warn("La instancia de connectionFactory es nula");

            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _retryCount = retryCount;
        }

        private static readonly Logger logger = LogManager.GetLogger(typeof(DefaultRabbitMQPersistentConnection).FullName);

        public bool IsConnected
        {
            get
            {
                logger.Info("Se inicia la ejecución del getter de la propiedad IsConnected");
                return _connection != null && _connection.IsOpen && !_disposed;
            }
        }

        public IModel CreateModel()
        {
            logger.Info("Se inicia la ejecución del método CreateModel");
            logger.Debug("Valor de la propiedad IsConnected - {}", IsConnected);

            if (!IsConnected)
            {
                logger.Warn("La conexión no ha sido abierta por lo que no puede crearse el model hacia el Message Broker");
                throw new InvalidOperationException("No RabbitMQ connections are available to perform this action");
            }

            return _connection.CreateModel();
        }

        public void Dispose()
        {
            logger.Info("Se inicia la ejecución del método Dispose");
            logger.Debug("Valor del campo _dispose - {}", _disposed);

            if (_disposed)
            {
                logger.Trace("La instancia ya ha sido dispuesta, se finaliza la ejecución del Dispose");
                return;
            }

            _disposed = true;
            logger.Trace("La instancia aún no ha sido dispuesta, se marca para realizar el Dispose");

            try
            {
                logger.Trace("Se realiza el Dispose de la conexión");
                _connection.Dispose();
            }
            catch (IOException ex)
            {
                logger.Error($"Ha ocurrido un error al realizar el Dispose: {ex.Message}", ex);
            }
        }

        public bool TryConnect()
        {
            logger.Info("Se inicia la ejecución del método TryConnect");

            lock (sync_root)
            {
                logger.Trace("Se ha obtenido el lock para intentar la conexión al Message Broker");
                var policy = RetryPolicy.Handle<SocketException>()
                    .Or<BrokerUnreachableException>()
                    .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                    {
                        logger.Warn("No se ha podido establecer la conexión al Message Broker");
                        logger.Debug("Argumentos del action onRetry: Exception - {}, TimeSpan - {}",
                            ex, time);
                    }
                );

                policy.Execute(() =>
                {
                    logger.Trace("Se intenta crear la conexión al Message Broker");
                    _connection = _connectionFactory
                          .CreateConnection();
                    logger.Debug("Valor de la variable _connection - {}", _connection);
                });

                logger.Debug("Valor de la propiedad IsConnected - {}", IsConnected);

                if (IsConnected)
                {
                    logger.Trace("Se asignan los event handlers a la conexión");
                    _connection.ConnectionShutdown += OnConnectionShutdown;
                    logger.Debug("Se ha establecido el manejador para ConnectionShutdown");
                    _connection.CallbackException += OnCallbackException;
                    logger.Debug("Se ha establecido el manejador para CallbackException");
                    _connection.ConnectionBlocked += OnConnectionBlocked;
                    logger.Debug("Se ha establecido el manejador para ConnectionShutdown");

                    logger.Trace("Se ha establecido la conexión al Message Broker: {}", _connection.Endpoint.HostName);
                    return true;
                }
                else
                {
                    logger.Warn("No se ha podido establecer la conexión al Message Broker");
                    return false;
                }
            }
        }

        void OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
            logger.Info("Se inicia la ejecución del event handler OnConnectionBlocked");
            logger.Debug("Argumentos del event handler OnConnectionBlocked: sender - {}, eventArgs - {}",
                sender, e);

            if (_disposed) 
            {
                logger.Trace("La instancia ha sido dispuesta, se finaliza la ejecución el event handler");
                return;
            }

            logger.Warn("La conexión al Message Broker ha sido bloqueada, intentando conectar");
            TryConnect();
        }

        void OnCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            logger.Info("Se inicia la ejecución del event handler OnCallbackException");
            logger.Debug("Argumentos del event handler OnCallbackException: sender - {}, eventArgs - {}",
                sender, e);

            if (_disposed) 
            {
                logger.Trace("La instancia ha sido dispuesta, se finaliza la ejecución el event handler");
                return;
            }

            logger.Warn("La conexión al Message Broker ha lanzado una excepción, intentando conectar");
            TryConnect();
        }

        void OnConnectionShutdown(object sender, ShutdownEventArgs reason)
        {
            logger.Info("Se inicia la ejecución del event handler OnConnectionShutdown");
            logger.Debug("Argumentos del event handler OnConnectionShutdown: sender -  {}, eventArgs - {}",
                sender, reason);

            if (_disposed) 
            {
                logger.Trace("La instancia ha sido dispuesta, se finaliza la ejecución el event handler");
                return;
            }

            logger.Warn("La conexión al Message Broker ha sido apagada, intentando conectar");
            TryConnect();
        }
    }
}
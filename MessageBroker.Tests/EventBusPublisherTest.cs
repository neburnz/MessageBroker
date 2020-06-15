using System;
using System.Linq;
using Docker.DotNet;
using Docker.DotNet.Models;
using RabbitMQ.Client;
using Xunit;
using MessageBroker.RabbitMQ;
using MessageBroker.Tests.Events;
using System.Threading.Tasks;
using Autofac;
using NLog;
using MessageBroker.Abstractions;

namespace SIFACE.Messaging.Tests
{
    public class EventBusPublisherTest : IDisposable
    {
        private readonly string HOST_NAME = "localhost";
        private readonly string VIRTUAL_HOST = "/";
        private readonly string USER_NAME = "app-user";
        private readonly string PASSWORD = "AppU$3r";
        private readonly int PORT = 5672;
        private readonly string QUEUE_NAME = "app_queue";
        private ConnectionFactory factory;
        private IContainer container;
        IRabbitMQPersistentConnection connection;
        IModel channel;
        public EventBusPublisherTest()
        {
            var builder = new ContainerBuilder();
            builder
                .Register(c => LogManager.GetLogger(typeof(EventBusPublisherTest).FullName))
                .As<Logger>();
            builder.RegisterInstance(new ConnectionFactory
            {
                HostName = HOST_NAME,
                VirtualHost = VIRTUAL_HOST,
                UserName = USER_NAME,
                Password = PASSWORD,
                Port = Convert.ToInt16(PORT)
            }).As<IConnectionFactory>();
            builder.RegisterType<DefaultRabbitMQPersistentConnection>().As<IRabbitMQPersistentConnection>().SingleInstance();
            builder.RegisterType<EventBusPublisher>().As<IEventBusPublisher>().PropertiesAutowired();
            container = builder.Build();

            StartMessageBrokerContainer().Wait();
            
            factory = new ConnectionFactory();
            factory.HostName = HOST_NAME;
            factory.VirtualHost = VIRTUAL_HOST;
            factory.UserName = USER_NAME;
            factory.Password = PASSWORD;
            factory.Port = PORT;

            connection = container.Resolve<IRabbitMQPersistentConnection>();
            connection.TryConnect();
            channel = connection.CreateModel();
        }

        private async Task StartMessageBrokerContainer()
        {
            using (var dockerClientConfiguration = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")))
            using (var dockerClient = dockerClientConfiguration.CreateClient())
            {
                var containers = await dockerClient.Containers.ListContainersAsync(new ContainersListParameters() { All = true });
                var container = containers.FirstOrDefault(c => c.Names.Contains("/" + "message-broker"));
                if (container.State != "running")
                {
                    var started = await dockerClient.Containers.StartContainerAsync(container.ID, new ContainerStartParameters());
                    if (!started)
                    {
                        Assert.True(false, "El contenedor del RabbitMQ no pudo ser iniciado");
                    }
                }
            }
        }

        private async Task StopMessageBrokerContainer()
        {
            using (var dockerClientConfiguration = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")))
            using (var dockerClient = dockerClientConfiguration.CreateClient())
            {
                var containers = await dockerClient.Containers.ListContainersAsync(new ContainersListParameters() { All = true });
                var container = containers.FirstOrDefault(c => c.Names.Contains("/" + "message-broker"));
                if (container.State == "running")
                {
                    var stopped = await dockerClient.Containers.StopContainerAsync(container.ID, new ContainerStopParameters());
                    if (!stopped)
                    {
                        Assert.True(false, "El contenedor del RabbitMQ no pudo ser detenido");
                    }
                }
            }
        }

        public void Dispose()
        {
            if (channel != null)
            {
                channel.QueuePurge(QUEUE_NAME);
                channel.Close();
                channel.Dispose();
            }
            if (connection.IsConnected)
            {
                connection.Dispose();
            }
            StopMessageBrokerContainer().Wait();
        }

        [Fact]
        public void When_Publish_Queue_Is_Not_Empty()
        {
            // Arrange
            var @event = new TestIntegrationEvent();
            var target = new EventBusPublisher(connection);
            // Act
            target.Publish(@event);
            // Assert
            var response = channel.QueueDeclarePassive(QUEUE_NAME);
            Assert.Equal(1u, response.MessageCount);
        }
    }
}
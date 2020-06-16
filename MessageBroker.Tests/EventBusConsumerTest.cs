using System;
using System.Linq;
using Docker.DotNet;
using Docker.DotNet.Models;
using RabbitMQ.Client;
using Xunit;
using MessageBroker.Abstractions;
using MessageBroker.RabbitMQ;
using MessageBroker.Tests.Events;
using System.Threading.Tasks;
using Autofac;
using NLog;

namespace MessageBroker.Tests
{
    public class EventBusConsumerTest : IDisposable
    {
        private readonly string HOST_NAME = "localhost";
        private readonly string VIRTUAL_HOST = "/";
        private readonly string USER_NAME = "app-user";
        private readonly string PASSWORD = "AppU$3r";
        private readonly int PORT = 5672;
        private readonly string QUEUE_NAME = "app_queue";
        private IContainer container;
        IRabbitMQPersistentConnection connection;
        IModel channel;
        public EventBusConsumerTest()
        {
            StartMessageBrokerContainer().Wait();

            var builder = new ContainerBuilder();
            builder.RegisterInstance(new ConnectionFactory
            {
                HostName = HOST_NAME,
                VirtualHost = VIRTUAL_HOST,
                UserName = USER_NAME,
                Password = PASSWORD,
                Port = Convert.ToInt16(PORT)
            }).As<IConnectionFactory>();
            builder.RegisterType<DefaultRabbitMQPersistentConnection>().As<IRabbitMQPersistentConnection>().SingleInstance();
            builder.RegisterType<InMemoryEventBusSubscriptionsManager>().As<IEventBusSubscriptionsManager>().SingleInstance();
            builder.Register(context =>
                new EventBusConsumer(
                    context.Resolve<IRabbitMQPersistentConnection>(),
                    context.Resolve<ILifetimeScope>(),
                    context.Resolve<IEventBusSubscriptionsManager>(),
                    QUEUE_NAME))
                .As<IEventBusConsumer>()
                .PropertiesAutowired()
                .SingleInstance();
            builder.RegisterType<EventBusPublisher>().As<IEventBusPublisher>().PropertiesAutowired();
            builder.RegisterType<TestIntegrationEventHandler>();
            builder.RegisterType<TestIntegrationFailedEventHandler>();
            container = builder.Build();

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
        public void When_Consumer_Raises_Exception_Then_Event_Is_Requeued()
        {
            // Arrange
            var response = channel.QueueDeclarePassive(QUEUE_NAME);
            var publisher = container.Resolve<IEventBusPublisher>();
            var @event = new TestIntegrationEvent();
            publisher.Publish(@event);

            // Act
            try
            {
                var target = container.Resolve<IEventBusConsumer>();
                target.Subscribe<TestIntegrationEvent, TestIntegrationFailedEventHandler>();
                Assert.Equal(0u, response.MessageCount);
            }
            catch (AggregateException)
            {
                // Assert
                Assert.Equal(1u, response.MessageCount);
            }
        }
        [Fact]
        public void When_Handler_Is_Executed_Then_Event_Is_Consumed()
        {
            // Arrange
            var publisher = container.Resolve<IEventBusPublisher>();
            var @event = new TestIntegrationEvent();
            publisher.Publish(@event);

            // Act
            var target = container.Resolve<IEventBusConsumer>();
            target.Subscribe<TestIntegrationEvent, TestIntegrationEventHandler>();

            // Assert
            var response = channel.QueueDeclarePassive(QUEUE_NAME);
            Assert.Equal(0u, response.MessageCount);
        }
    }
}
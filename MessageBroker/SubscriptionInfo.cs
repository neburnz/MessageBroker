using System;
using NLog;

namespace MessageBroker
{
    public partial class InMemoryEventBusSubscriptionsManager : IEventBusSubscriptionsManager
    {
        public class SubscriptionInfo
        {
            private static readonly Logger logger = LogManager.GetLogger(typeof(SubscriptionInfo).FullName);
            public Type HandlerType { get; }
            private SubscriptionInfo(Type handlerType)
            {
                logger.Info("Se inicia la ejecución del constructor");
                logger.Debug("Argumentos del constructor: handlerType - {}", handlerType);

                if (handlerType == null)
                    logger.Warn("La instancia de handlerType es nula");

                HandlerType = handlerType;
            }
            public static SubscriptionInfo Typed(Type handlerType)
            {
                logger.Info("Se inicia la ejecución del método Typed");
                logger.Debug("Argumentos del método Typed: handlerType - {}", handlerType);

                logger.Trace("Se genera una nueva instancia de SubscriptionInfo");
                logger.Debug("Valor del argumento del constructor SubscriptionInfo: handlerType - {}", handlerType);
                var result = new SubscriptionInfo(handlerType);
                logger.Debug("Valor de la variable {} - {}", nameof(result), result);

                logger.Trace("Se retorna la instancia de SubscriptionInfo");
                return result;
            }
        }
    }
}
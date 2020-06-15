using System;
using System.Collections.Generic;
using System.Linq;
using NLog;
using MessageBroker.Events;

namespace MessageBroker
{
    public partial class InMemoryEventBusSubscriptionsManager : IEventBusSubscriptionsManager
    {
        private static readonly Logger logger = LogManager.GetLogger(typeof(InMemoryEventBusSubscriptionsManager).FullName);
        private readonly Dictionary<string, List<SubscriptionInfo>> _handlers;
        private readonly List<Type> _eventTypes;
        public event EventHandler<string> OnEventRemoved;
        public InMemoryEventBusSubscriptionsManager()
        {
            logger.Info("Se inicia la ejecución del constructor");
            
            logger.Trace("Se inicializa el campo {}", nameof(_handlers));
            _handlers = new Dictionary<string, List<SubscriptionInfo>>();
            logger.Trace("Se inicializa el campo {}", nameof(_eventTypes));
            _eventTypes = new List<Type>();
        }
        public bool IsEmpty
        {
            get
            {
                var method = System.Reflection.MethodBase.GetCurrentMethod();
                logger.Info($"Se inicia la ejecución del método {method.Name}");

                logger.Trace("Se verifica si la colección de {} contiene elementos", nameof(_handlers));
                var result = !_handlers.Keys.Any();
                logger.Debug("El valor de la variable {} - {}", nameof(result), result);
                return result;
            }
        }
        public void Clear() 
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se realiza la limpieza de la colección {}", nameof(_handlers));
            _handlers.Clear(); 
        }
        public void AddSubscription<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se obtiene el nombre del evento");
            var eventName = GetEventKey<T>();
            logger.Debug("Valor de la variable eventName - {}", eventName);

            logger.Trace("Se intenta agregar una suscripción");
            logger.Debug("Argumentos de DoAddSubscription: handlerType - {}, eventName - {}, isDynamic - {}",
                typeof(TH), eventName, false);
            DoAddSubscription(typeof(TH), eventName, isDynamic: false);
            
            logger.Trace("Se agrega el tipo del evento a la lista de _eventTypes");
            logger.Debug("Argumentos de _eventTypes.Add: item - {}", typeof(T));
            _eventTypes.Add(typeof(T));
        }
        private void DoAddSubscription(Type handlerType, string eventName, bool isDynamic)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}, {} - {}, {} - {}",
                method.Name, nameof(handlerType), handlerType, nameof(eventName), eventName, nameof(isDynamic), isDynamic);

            if (handlerType == null)
                logger.Warn($"La instancia del argumento {nameof(handlerType)} es nula");
            if (string.IsNullOrWhiteSpace(eventName))
                logger.Warn($"La instancia del argumento {nameof(eventName)} es nula o vacía");

            logger.Trace("Se verifica si existen suscripciones para el evento: {}", eventName);
            if (!HasSubscriptionsForEvent(eventName))
            {
                logger.Trace("No existen suscripciones para el evento: {} por lo que se agrega la entrada a la colección", eventName);
                logger.Debug("Argumentos de _handlers.Add: key - {}, value - {}", eventName, new List<SubscriptionInfo>());
                _handlers.Add(eventName, new List<SubscriptionInfo>());
            }

            logger.Trace("Se verifica si existe un handler registrado para el evento: {} del tipo: {}", eventName, handlerType);
            if (_handlers[eventName].Any(s => s.HandlerType == handlerType))
            {
                logger.Warn($"Ya se ha registrado el handler: {handlerType.Name} para el evento: {eventName}");
                throw new ArgumentException(
                    $"El handler {handlerType.Name} ya ha sido registrado para el evento '{eventName}'", nameof(handlerType));
            }

            logger.Trace("Se agrega el handler: {} a la lista de suscripciones del evento: {}", handlerType.Name, eventName);
            logger.Debug("Argumentos de _handlers[{}].Add: item - {}", eventName, handlerType);
            _handlers[eventName].Add(SubscriptionInfo.Typed(handlerType));
        }
        public void RemoveSubscription<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se obtienen las suscripciones del evento y handlers para remover");
            var handlerToRemove = FindSubscriptionToRemove<T, TH>();
            logger.Debug("Valor de la variable {} - {}", nameof(handlerToRemove), handlerToRemove);

            logger.Trace("Se obtiene el nombre del Evento");
            var eventName = GetEventKey<T>();
            logger.Debug("Valor de la variable {} - {}", nameof(eventName), eventName);

            logger.Trace("Se procede con la remoción del handler");
            DoRemoveHandler(eventName, handlerToRemove);
        }
        private void DoRemoveHandler(string eventName, SubscriptionInfo subsToRemove)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}, {} - {}",
                method.Name, nameof(eventName), eventName, nameof(subsToRemove), subsToRemove);

            if (string.IsNullOrWhiteSpace(eventName))
                logger.Warn($"La instancia del argumento {nameof(eventName)} es nula o vacía");
            if (subsToRemove == null)
                logger.Warn($"La instancia del argumento {nameof(subsToRemove)} es nula");

            logger.Trace("Se verifica que existan suscripciones para remover de la colección de {}", nameof(_handlers));
            if (subsToRemove != null)
            {
                logger.Trace("Se han encontrado suscripciones para remover de la colección de {}", nameof(_handlers));
                logger.Debug("Argumentos de {}[{}].Remove: {} - {}",
                    nameof(_handlers), eventName, subsToRemove);
                _handlers[eventName].Remove(subsToRemove);

                logger.Trace("Se verifica que no existan suscripciones en la colección {} para el evento {}", nameof(_handlers), eventName);
                if (!_handlers[eventName].Any())
                {
                    logger.Trace("No existen suscripciones en la colección {} para el evento {}", nameof(_handlers), eventName);
                    
                    logger.Debug("Se remueve la key {} de la colección de {}", eventName, nameof(_handlers));
                    _handlers.Remove(eventName);

                    logger.Trace("Se verifica que que el evento {} se encuentra en la colección de {}", eventName, nameof(_eventTypes));
                    var eventType = _eventTypes.SingleOrDefault(e => e.Name == eventName);
                    logger.Debug("Valor de la variable {} - {}", nameof(eventType), eventType);

                    if (eventType != null)
                    {
                        logger.Debug("Se procede a remover el tipo de evento {} de la colección de {}", eventType, nameof(_eventTypes));
                        _eventTypes.Remove(eventType);
                    }

                    logger.Trace("Se realiza la invocación del evento EventRemoved");
                    RaiseOnEventRemoved(eventName);
                }
            }
        }
        public IEnumerable<SubscriptionInfo> GetHandlersForEvent<T>() where T : IntegrationEvent
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se obtiene la clave del evento para su consulta");
            var key = GetEventKey<T>();
            logger.Debug("Valor de la variable {} - {}", nameof(key), key);

            logger.Trace("Se obtienen la lista de handlers para el evento con clave {}", key);
            var result = GetHandlersForEvent(key);
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna la lista de handlers del evento {}", key);
            return result;
        }
        public IEnumerable<SubscriptionInfo> GetHandlersForEvent(string eventName)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}",
                method.Name, nameof(eventName), eventName);

            logger.Trace("Se buscan los handlers en la colección de {} por la clave {}", nameof(_handlers), eventName);
            var result = _handlers[eventName];
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna la lista de handlers encontrados para el evento {}", eventName);
            return result;
        }
        private void RaiseOnEventRemoved(string eventName)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}",
                method.Name, nameof(eventName), eventName);

            logger.Trace("Se obtiene la instancia del event {}", nameof(OnEventRemoved));
            var handler = OnEventRemoved;
            logger.Debug("Valor de la variable {} - {}", nameof(handler), handler);

            if (handler == null)
                logger.Warn($"La instancia del event {nameof(OnEventRemoved)} es nula");

            logger.Trace("Se verifica que se haya inicializado el event {}", nameof(OnEventRemoved));
            if (handler != null)
            {
                logger.Trace("Se procede a lanzar el event {}", nameof(OnEventRemoved));
                logger.Debug("Argumentos de OnEventRemoved: sender - {}, eventArgs - {}", this, eventName);
                OnEventRemoved(this, eventName);
            }
        }
        private SubscriptionInfo FindSubscriptionToRemove<T, TH>()
             where T : IntegrationEvent
             where TH : IIntegrationEventHandler<T>
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se obtiene el nombre del evento");
            var eventName = GetEventKey<T>();
            logger.Debug("Valor de la variable {} - {}", nameof(eventName), eventName);

            logger.Trace("Se realiza la consulta de suscripciones para su remoción");
            logger.Debug("Argumentos de DoFindSubscriptionToRemove: eventName - {}, handlerType - {}",
                eventName, typeof(TH));
            var result = DoFindSubscriptionToRemove(eventName, typeof(TH));
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna la lista de suscripciones para su remoción");
            return result;
        }
        private SubscriptionInfo DoFindSubscriptionToRemove(string eventName, Type handlerType)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}, {} - {}",
                method.Name, nameof(eventName), eventName, nameof(handlerType), handlerType);

            if (string.IsNullOrWhiteSpace(eventName))
                logger.Warn($"La instancia del argumento {nameof(eventName)} es nula o vacía");
            if (handlerType == null)
                logger.Warn($"La instancia del argumento {nameof(handlerType)} es nula");

            logger.Trace("Se verifica si existen suscripciones para el evento {}", eventName);
            if (!HasSubscriptionsForEvent(eventName))
            {
                logger.Debug("No se encontraron suscripciones para el evento {}, se retorna un valor nulo", eventName);
                return null;
            }

            logger.Trace("Se realiza la búsqueda de los handlers de la colección {}", nameof(_handlers));
            logger.Debug("Argumentos de {}}[{}}].SingleOrDefault: {} - {}", nameof(_handlers), eventName, nameof(handlerType), handlerType);
            var result = _handlers[eventName].SingleOrDefault(s => s.HandlerType == handlerType);
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna la lista de handlers");
            return result;
        }
        public bool HasSubscriptionsForEvent<T>() where T : IntegrationEvent
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");

            logger.Trace("Se obtiene la clave basado en el tipo de evento");
            var key = GetEventKey<T>();
            logger.Debug("Valor de la variable {} - {}", nameof(key), key);

            logger.Trace("Se verifica si existen suscripciones para el evento {}", key);
            var result = HasSubscriptionsForEvent(key);
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna el valor que indica si existen suscripciones para el evento {}", key);
            return result;
        }
        public bool HasSubscriptionsForEvent(string eventName)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}",
                method.Name, nameof(eventName), eventName);

            if (string.IsNullOrWhiteSpace(eventName))
                logger.Warn($"La instancia del argumento {nameof(eventName)} es nula o vacía");

            logger.Trace("Se verifica si el evento {} existe en la colección {}", eventName, nameof(_handlers));
            var result = _handlers.ContainsKey(eventName);
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna el valor que indica si existen suscripciones para el evento {}", eventName);
            return result;
        }
        public Type GetEventTypeByName(string eventName)
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            logger.Debug("Argumentos del método {}: {} - {}",
                method.Name, nameof(eventName), eventName);

            if (string.IsNullOrWhiteSpace(eventName))
                logger.Warn($"La instancia del argumento {nameof(eventName)} es nula o vacía");

            logger.Trace("Se consulta si existe el evento {} en la colección {}", eventName, nameof(_eventTypes));
            var result = _eventTypes.SingleOrDefault(t => t.Name == eventName);
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna el tipo del evento {}", eventName);
            return result;
        }
        public string GetEventKey<T>()
        {
            var method = System.Reflection.MethodBase.GetCurrentMethod();
            logger.Info($"Se inicia la ejecución del método {method.Name}");
            
            logger.Trace("Se obtiene la clave del tipo de evento");
            var result = typeof(T).Name;
            logger.Debug("Valor de la variable {} - {}", nameof(result), result);

            logger.Trace("Se retorna el valor de la clave del evento");
            return result;
        }
    }
}
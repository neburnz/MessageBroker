using System;
using System.Runtime.Serialization;
using System.Security.Permissions;

namespace MessageBroker.Events
{
    [Serializable]
    public class IntegrationEvent : ISerializable
    {
        public IntegrationEvent()
        {
            Id = Guid.NewGuid();
            CreationDate = DateTime.UtcNow;
        }

        /// <summary>
        /// Special constructor that is used when the object is deserialized.
        /// </summary>
        /// <param name="info">The SerializationInfo to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        protected IntegrationEvent(SerializationInfo info, StreamingContext context)
        {
            Id = Guid.Parse(info.GetString(nameof(Id)));
            CreationDate = info.GetDateTime(nameof(CreationDate));
        }

        /// <summary>
        /// Populates a SerializationInfo with the data needed to serialize the target object.
        /// </summary>
        /// <param name="info">The SerializationInfo to populate with data.</param>
        /// <param name="context">The destination (see StreamingContext) for this serialization.</param>
        [SecurityPermissionAttribute(SecurityAction.Demand, SerializationFormatter = true)]
        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue(nameof(Id), Id);
            info.AddValue(nameof(CreationDate), CreationDate);
        }

        public Guid Id { get; }

        public DateTime CreationDate { get; }
    }
}

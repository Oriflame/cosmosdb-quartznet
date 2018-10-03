using System;
using Newtonsoft.Json;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class PersistentLock : QuartzEntityBase
    {       
        public const string EntityType = "Lock";
        
        public DateTimeOffset AcquiredAt { get; set; }
        
        public LockType LockType { get; set; }
        
        public string InstanceId { get; set; }
        
        /// <summary>
        /// Used to set expiration policy for failover. [s]
        /// </summary>
        [JsonProperty(PropertyName = "ttl", NullValueHandling = NullValueHandling.Ignore)]
        public int? Ttl { get; set; }
        
        
        protected PersistentLock()
        {
        }

        public PersistentLock(string instanceName, LockType lockType, DateTimeOffset acquiredAt, string instanceId, int? ttl) : 
            base(EntityType, $"{EntityType}-{instanceName}-{lockType}", instanceName)
        {
            LockType = lockType;
            AcquiredAt = acquiredAt;
            InstanceId = instanceId;
            Ttl = ttl;
        }
    }
}
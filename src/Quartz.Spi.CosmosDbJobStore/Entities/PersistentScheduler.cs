using System;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    /// <summary>
    /// 
    /// </summary>
    public class PersistentScheduler : QuartzEntityBase
    {       
        public const string EntityType = "Scheduler";
        
        public DateTimeOffset LastCheckIn { get; set; }
        
        public TimeSpan CheckinInterval { get; set; }
        
        public string InstanceId { get; set; }
        
        
        
        protected PersistentScheduler()
        {
        }

        public PersistentScheduler(string instanceName, string instanceId) : 
            base(EntityType, GetId(instanceName, instanceId), instanceName)
        {
            InstanceId = instanceId;
        }


        public static string GetId(string instanceName, string instanceId)
        {
            return $"{EntityType}-{instanceName}-{instanceId}";
        }
    }
}
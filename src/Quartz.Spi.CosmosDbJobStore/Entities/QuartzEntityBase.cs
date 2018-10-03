using Newtonsoft.Json;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    public abstract class QuartzEntityBase
    {
        [JsonProperty("id")]
        public string Id { get; set; }
        
        [JsonProperty("type")]
        public string Type { get; set; }
        
        [JsonProperty("instanceName")]
        public string InstanceName { get; set; }

        
        protected QuartzEntityBase()
        {
        }

        protected QuartzEntityBase(string type, string id, string instanceName)
        {
            Id = id;
            Type = type;
            InstanceName = instanceName;
        }
    }
}
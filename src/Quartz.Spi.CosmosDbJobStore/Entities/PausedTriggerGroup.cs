namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    public class PausedTriggerGroup : QuartzEntityBase
    {
        public const string EntityType = "PausedTriggerGroup";
        
        public string Group { get; set; }
        
        
        protected PausedTriggerGroup()
        {
        }

        public PausedTriggerGroup(string group, string instanceName) : base(EntityType, GetId(instanceName, group), instanceName)
        {
            Group = group;
        }
        
        
        public static string GetId(string instanceName, string group)
        {
            return $"{EntityType}-{instanceName}-{group}";
        }
    }
}
using System;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal abstract class PersistentTriggerBase : QuartzEntityBase
    {
        public const string EntityType = "Trigger";
        
        public string Group { get; set; }
        
        public string Name { get; set; }
        
        public string JobGroup { get; set; }
        
        public string JobName { get; set; }
        
        public string Description { get; set; }

        public DateTimeOffset? NextFireTime { get; set; }

        public DateTimeOffset? PreviousFireTime { get; set; }

        public PersistentTriggerState State { get; set; }

        public DateTimeOffset StartTime { get; set; }

        public DateTimeOffset? EndTime { get; set; }

        public string CalendarName { get; set; }

        public int MisfireInstruction { get; set; }

        public int Priority { get; set; }

        public JobDataMap JobDataMap { get; set; }

        
        protected PersistentTriggerBase()
        {
        }

        protected PersistentTriggerBase(ITrigger trigger, PersistentTriggerState state, string instanceName) : 
                        base(EntityType, GetId(instanceName, trigger.Key.Group, trigger.Key.Name), instanceName)
        {
            Group = trigger.Key.Group;
            Name = trigger.Key.Name;
            JobGroup = trigger.JobKey.Group;
            JobName = trigger.JobKey.Name;
            Description = trigger.Description;
            NextFireTime = trigger.GetNextFireTimeUtc()?.ToUniversalTime();
            PreviousFireTime = trigger.GetPreviousFireTimeUtc()?.ToUniversalTime();
            State = state;
            StartTime = trigger.StartTimeUtc;
            EndTime = trigger.EndTimeUtc;
            CalendarName = trigger.CalendarName;
            MisfireInstruction = trigger.MisfireInstruction;
            Priority = trigger.Priority;
            JobDataMap = trigger.JobDataMap;
        }
        
        
        protected void FillTrigger(AbstractTrigger trigger)
        {
            trigger.Key = GetTriggerKey();
            trigger.JobKey = GetJobKey();
            trigger.CalendarName = CalendarName;
            trigger.Description = Description;
            trigger.JobDataMap = JobDataMap;
            trigger.MisfireInstruction = MisfireInstruction;
            trigger.EndTimeUtc = null; // Avoid <> checks
            trigger.StartTimeUtc = StartTime;
            trigger.EndTimeUtc = EndTime;
            trigger.Priority = Priority;
            trigger.SetNextFireTimeUtc(NextFireTime?.ToUniversalTime());
            trigger.SetPreviousFireTimeUtc(PreviousFireTime?.ToUniversalTime());
        }
        
        public abstract ITrigger GetTrigger();

        public TriggerKey GetTriggerKey()
        {
            return new TriggerKey(Name, Group);
        }
        
        public JobKey GetJobKey()
        {
            return new JobKey(JobName, JobGroup);
        }
        
        
        public static string GetId(string instanceName, TriggerKey key)
        {
            return GetId(instanceName, key.Group, key.Name);
        }

        public static string GetId(string instanceName, string group, string name)
        {
            return $"{EntityType}-{instanceName}-{group}-{name}";
        }
    }
}
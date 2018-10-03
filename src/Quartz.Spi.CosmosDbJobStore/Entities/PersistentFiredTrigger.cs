using System;
using System.Globalization;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal class PersistentFiredTrigger : QuartzEntityBase
    {
        public const string EntityType = "FiredTrigger";
        
        public string FireInstanceId { get; set; }

        public string TriggerName { get; set; }
        
        public string TriggerGroup { get; set; }

        public string JobName { get; set; }
        
        public string JobGroup { get; set; }

        public string InstanceId { get; set; }

        public DateTimeOffset Fired { get; set; }

        public DateTimeOffset? Scheduled { get; set; }

        public int Priority { get; set; }

        public PersistentTriggerState State { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }

        
        protected PersistentFiredTrigger()
        {
        }
        
        public PersistentFiredTrigger(string firedInstanceId, PersistentTriggerBase trigger, PersistentJob job) : base(EntityType, GetId(trigger.InstanceName, firedInstanceId), trigger.InstanceName)
        {
            FireInstanceId = firedInstanceId;
            TriggerName = trigger.Name;
            TriggerGroup = trigger.Group;
            Fired = DateTimeOffset.Now;
            Scheduled = trigger.NextFireTime;
            Priority = trigger.Priority;
            State = trigger.State;

            if (job != null)
            {
                JobName = job.Name;
                JobGroup = job.Group;
                ConcurrentExecutionDisallowed = job.ConcurrentExecutionDisallowed;
                RequestsRecovery = job.RequestsRecovery;
            }
        }

        public TriggerKey GetTriggerKey()
        {
            return new TriggerKey(TriggerName, TriggerGroup);
        }
        
        public JobKey GetJobKey()
        {
            return JobName == null ? null : new JobKey(JobName, JobGroup);
        }
        
        public IOperableTrigger GetRecoveryTrigger(JobDataMap jobDataMap)
        {
            var firedTime = Fired;
            var scheduledTime = Scheduled.GetValueOrDefault(DateTimeOffset.MinValue);
            var recoveryTrigger = new SimpleTriggerImpl($"recover_{InstanceId}_{Guid.NewGuid()}",
                SchedulerConstants.DefaultRecoveryGroup, scheduledTime)
            {
                JobName = JobName,
                JobGroup = JobGroup,
                Priority = Priority,
                MisfireInstruction = MisfireInstruction.IgnoreMisfirePolicy,
                JobDataMap = jobDataMap
            };
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerName, TriggerName);
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, TriggerGroup);
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerFiretime,
                Convert.ToString(firedTime, CultureInfo.InvariantCulture));
            recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerScheduledFiretime,
                Convert.ToString(scheduledTime, CultureInfo.InvariantCulture));
            return recoveryTrigger;
        }

        public static string GetId(string instanceName, string firedInstanceId)
        {
            return $"{EntityType}-{instanceName}-{firedInstanceId}";
        }
    }
}
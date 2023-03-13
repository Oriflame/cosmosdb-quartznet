using System;
using Quartz.Impl;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    public class PersistentJob : QuartzEntityBase
    {
        public const string EntityType = "JobDetail";
        
        public string Name { get; set; }
        
        public string Group { get; set; }
        
        public string Description { get; set; }

        public Type JobType { get; set; }

        public JobDataMap JobDataMap { get; set; }

        public bool Durable { get; set; }

        public bool PersistJobDataAfterExecution { get; set; }

        public bool ConcurrentExecutionDisallowed { get; set; }

        public bool RequestsRecovery { get; set; }

        
        protected PersistentJob()
        {
        }

        public PersistentJob(IJobDetail jobDetail, string instanceName) : base(EntityType, GetId(instanceName, jobDetail.Key.Group, jobDetail.Key.Name), instanceName)
        {
            Name = jobDetail.Key.Name;
            Group = jobDetail.Key.Group;
            Description = jobDetail.Description;
            JobType = jobDetail.JobType;
            JobDataMap = jobDetail.JobDataMap;
            Durable = jobDetail.Durable;
            PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }

        
        public IJobDetail GetJobDetail()
        {
            // The missing properties are figured out at runtime from the job type attributes
            return JobBuilder.Create(JobType)
                .WithIdentity(GetJobKey())
                .WithDescription(Description)
                .SetJobData(JobDataMap)
                .StoreDurably(Durable)
                .RequestRecovery(RequestsRecovery)
                .Build();
        }

        public JobKey GetJobKey()
        {
            return new JobKey(Name, Group);
        }

        
        public static string GetId(string instanceName, JobKey key)
        {
            return GetId(instanceName, key.Group, key.Name);
        }

        public static string GetId(string instanceName, string group, string name)
        {
            return $"{EntityType}-{instanceName}-{group}-{name}";
        }
    }
}
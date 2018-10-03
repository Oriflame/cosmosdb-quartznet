using System;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal class PersistentCronTrigger : PersistentTriggerBase
    {
        protected PersistentCronTrigger()
        {
        }

        public PersistentCronTrigger(ICronTrigger trigger, PersistentTriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            CronExpression = trigger.CronExpressionString;
            TimeZone = trigger.TimeZone.Id;
        }

        
        public string CronExpression { get; set; }

        public string TimeZone { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new CronTriggerImpl()
            {
                CronExpressionString = CronExpression,
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone)
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}
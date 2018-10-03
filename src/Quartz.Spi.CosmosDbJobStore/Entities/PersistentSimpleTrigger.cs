using System;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal class PersistentSimpleTrigger : PersistentTriggerBase
    {
        protected PersistentSimpleTrigger()
        {
        }

        public PersistentSimpleTrigger(ISimpleTrigger trigger, PersistentTriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            RepeatCount = trigger.RepeatCount;
            RepeatInterval = trigger.RepeatInterval;
            TimesTriggered = trigger.TimesTriggered;
        }

        public int RepeatCount { get; set; }

        public TimeSpan RepeatInterval { get; set; }

        public int TimesTriggered { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new SimpleTriggerImpl()
            {
                RepeatCount = RepeatCount,
                RepeatInterval = RepeatInterval,
                TimesTriggered = TimesTriggered
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}
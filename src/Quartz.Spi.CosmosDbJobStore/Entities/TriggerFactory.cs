using System;
using Microsoft.Azure.Documents;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    public class TriggerFactory
    {
        internal static PersistentTriggerBase CreateTrigger(ITrigger trigger, Entities.PersistentTriggerState state, string instanceName)
        {
            if (trigger is ICronTrigger cronTrigger)
            {
                return new PersistentCronTrigger(cronTrigger, state, instanceName);
            }
            if (trigger is ISimpleTrigger simpleTrigger)
            {
                return new PersistentSimpleTrigger(simpleTrigger, state, instanceName);
            }
            if (trigger is ICalendarIntervalTrigger intervalTrigger)
            {
                return new PersistentCalendarIntervalTrigger(intervalTrigger, state, instanceName);
            }
            if (trigger is IDailyTimeIntervalTrigger timeIntervalTrigger)
            {
                return new PersistentDailyTimeIntervalTrigger(timeIntervalTrigger, state, instanceName);
            }

            throw new NotSupportedException($"Trigger of type {trigger.GetType().FullName} is not supported");
        }
    }
}
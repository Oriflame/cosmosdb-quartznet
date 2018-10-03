using System;
using System.Collections.Generic;
using Quartz.Impl.Triggers;

namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal class PersistentDailyTimeIntervalTrigger : PersistentTriggerBase
    {
        protected PersistentDailyTimeIntervalTrigger()
        {
        }

        public PersistentDailyTimeIntervalTrigger(IDailyTimeIntervalTrigger trigger, PersistentTriggerState state, string instanceName)
            : base(trigger, state, instanceName)
        {
            RepeatCount = trigger.RepeatCount;
            RepeatIntervalUnit = trigger.RepeatIntervalUnit;
            RepeatInterval = trigger.RepeatInterval;
            StartTimeOfDay = trigger.StartTimeOfDay;
            EndTimeOfDay = trigger.EndTimeOfDay;
            DaysOfWeek = new HashSet<DayOfWeek>(trigger.DaysOfWeek);
            TimesTriggered = trigger.TimesTriggered;
            TimeZone = trigger.TimeZone.Id;
        }

        public int RepeatCount { get; set; }

        public IntervalUnit RepeatIntervalUnit { get; set; }

        public int RepeatInterval { get; set; }

        public TimeOfDay StartTimeOfDay { get; set; }

        public TimeOfDay EndTimeOfDay { get; set; }

        public HashSet<DayOfWeek> DaysOfWeek { get; set; }

        public int TimesTriggered { get; set; }

        public string TimeZone { get; set; }

        public override ITrigger GetTrigger()
        {
            var trigger = new DailyTimeIntervalTriggerImpl
            {
                RepeatCount = RepeatCount,
                RepeatIntervalUnit = RepeatIntervalUnit,
                RepeatInterval = RepeatInterval,
                StartTimeOfDay = StartTimeOfDay ?? new TimeOfDay(0, 0, 0),
                EndTimeOfDay = EndTimeOfDay ?? new TimeOfDay(23, 59, 59),
                DaysOfWeek = new HashSet<DayOfWeek>(DaysOfWeek),
                TimesTriggered = TimesTriggered,
                TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone)
            };
            FillTrigger(trigger);
            return trigger;
        }
    }
}
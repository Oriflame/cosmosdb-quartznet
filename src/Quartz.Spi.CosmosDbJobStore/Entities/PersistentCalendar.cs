namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    internal class PersistentCalendar : QuartzEntityBase
    {
        public const string EntityType = "Calendar";
        
        public string CalendarName { get; set; }
        
        public ICalendar Calendar { get; set; }

        
        protected PersistentCalendar()
        {
        }

        
        public PersistentCalendar(string instanceName, string calendarName, ICalendar calendar) : 
            base(EntityType, GetId(instanceName, calendarName), instanceName)
        {
            CalendarName = calendarName;
            Calendar = calendar;
        }

        
        public static string GetId(string instanceName, string calendarName)
        {
            return $"{EntityType}-{instanceName}-{calendarName}";
        }
    }
}
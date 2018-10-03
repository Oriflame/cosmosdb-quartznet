namespace Quartz.Spi.CosmosDbJobStore.Entities
{
    public enum PersistentTriggerState
    {
        None = 0,
        Waiting = 1,
        Acquired = 2,
        Executing = 3,
        Complete = 4,
        Blocked = 5,
        Error = 6,
        Paused = 7,
        PausedBlocked = 8,
        Deleted = 9
    }
}
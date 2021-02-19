using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class SchedulerRepository : CosmosDbRepositoryBase<PersistentScheduler>
    {
        public SchedulerRepository(Container container, string instanceName, bool partitionPerEntityType)
            : base(container, PersistentScheduler.EntityType, instanceName, partitionPerEntityType)
        {
        }
    }
}

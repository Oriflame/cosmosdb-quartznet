using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class SchedulerRepository : CosmosDbRepositoryBase<PersistentScheduler>
    {
        public SchedulerRepository(IDocumentClient documentClient, string databaseId, string collectionId, string instanceName)
            : base(documentClient, databaseId, collectionId, PersistentScheduler.EntityType, instanceName)
        {
        }
    }
}
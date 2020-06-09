using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class JobRepository : CosmosDbRepositoryBase<PersistentJob>
    {
        public JobRepository(IDocumentClient documentClient, string databaseId, string collectionId,
            string instanceName, bool partitionPerEntityType)
            : base(documentClient, databaseId, collectionId, PersistentJob.EntityType, instanceName, partitionPerEntityType)
        {
        }


        public Task<IReadOnlyCollection<string>> GetGroups()
        {
            return Task.FromResult<IReadOnlyCollection<string>>(_documentClient
                .CreateDocumentQuery<PersistentJob>(_collectionUri, FeedOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .AsEnumerable()
                .Distinct()
                .ToList());
        }
    }
}

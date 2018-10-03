using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class PausedTriggerGroupRepository : CosmosDbRepositoryBase<PausedTriggerGroup>
    {
        public PausedTriggerGroupRepository(IDocumentClient documentClient, string databaseId, string collectionId, string instanceName)
            : base(documentClient, databaseId, collectionId, PausedTriggerGroup.EntityType, instanceName)
        {
        }

        
        public async Task<IReadOnlyCollection<string>> GetGroups()
        {
            return _documentClient.CreateDocumentQuery<PausedTriggerGroup>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .AsEnumerable()
                .ToList();
        }
    }
}
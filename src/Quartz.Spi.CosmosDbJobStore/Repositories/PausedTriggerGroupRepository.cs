using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class PausedTriggerGroupRepository : CosmosDbRepositoryBase<PausedTriggerGroup>
    {
        public PausedTriggerGroupRepository(Container container, string instanceName, bool partitionPerEntityType)
            : base(container, PausedTriggerGroup.EntityType, instanceName, partitionPerEntityType)
        {
        }

        
        public Task<IReadOnlyCollection<string>> GetGroups()
        {
            return Task.FromResult<IReadOnlyCollection<string>>(_container
                .GetItemLinqQueryable<PausedTriggerGroup>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .AsEnumerable()
                .ToList());
        }
    }
}

using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    public class LockRepository : CosmosDbRepositoryBase<PersistentLock>
    {
        public LockRepository(Container container, string instanceName, bool partitionPerEntityType)
            : base(container, PersistentLock.EntityType, instanceName, partitionPerEntityType)
        {
        }

        public async Task<bool> TrySave(PersistentLock lck)
        {
            try
            {
                await _container.CreateItemAsync(lck, _partitionKey);
                return true;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.Conflict)
            {
                return false;
            }
        }

        public Task<bool> TryDelete(string lockId)
        {
            return Delete(lockId);
        }
        
        public Task<IList<PersistentLock>> GetAllByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentLock>>(_container
                .GetItemLinqQueryable<PersistentLock>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .AsEnumerable()
                .ToList());
        }
    }
}

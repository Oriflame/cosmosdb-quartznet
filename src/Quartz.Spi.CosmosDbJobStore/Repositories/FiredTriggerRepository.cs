using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class FiredTriggerRepository : CosmosDbRepositoryBase<PersistentFiredTrigger>
    {
        public FiredTriggerRepository(Container container, string instanceName, bool partitionPerEntityType)
            : base(container, PersistentFiredTrigger.EntityType, instanceName, partitionPerEntityType)
        {
        }


        public Task<IList<PersistentFiredTrigger>> GetAllByJob(string jobName, string jobGroup)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName)
                .AsEnumerable()
                .ToList());
        }

        public Task<IList<PersistentFiredTrigger>> GetAllRecoverableByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId && x.RequestsRecovery)
                .AsEnumerable()
                .ToList());
        }
        
        public Task<IList<PersistentFiredTrigger>> GetAllByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .AsEnumerable()
                .ToList());
        }

        /// <summary>
        /// Deletes FiredTrigger + 
        /// </summary>
        /// <param name="instanceId"></param>
        /// <returns></returns>
        public async Task<int> DeleteAllByInstanceId(string instanceId)
        {
            // We may introduce paging if performance boost is necessary
            
            var triggers = _container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .Select(x => x.Id)
                .AsEnumerable()
                .ToList();

            foreach (var trigger in triggers)
            {
                await Delete(trigger);
            }
            
            return triggers.Count;
        }

        public Task<IList<PersistentFiredTrigger>> GetAllByTrigger(string triggerKeyName, string triggerKeyGroup)
        {
            return Task.FromResult<IList<PersistentFiredTrigger>>(_container
                .GetItemLinqQueryable<PersistentFiredTrigger>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.TriggerGroup == triggerKeyGroup && (x.TriggerName == null || x.TriggerName == triggerKeyName))
                .AsEnumerable()
                .ToList());
        }
    }
}

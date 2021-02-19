using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.Azure.Cosmos;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    public abstract class CosmosDbRepositoryBase<TEntity> where TEntity : QuartzEntityBase
    {
        protected static readonly ILog _logger = LogManager.GetLogger<CosmosDbRepositoryBase<TEntity>>();

        protected readonly string _type;
        protected readonly string _instanceName;
        protected readonly Container _container;
        protected readonly QueryRequestOptions _queryRequestOptions;
        protected readonly PartitionKey _partitionKey;
        

        protected CosmosDbRepositoryBase(
            Container container,
            string type,
            string instanceName,
            bool partitionPerEntityType)
        {
            _instanceName = instanceName;
            _container = container;
            _type = type;
            _partitionKey = new PartitionKey(partitionPerEntityType ? type : instanceName);
            
            _queryRequestOptions = new QueryRequestOptions
            {
                PartitionKey = _partitionKey,
                /*
                    For queries, the value of MaxItemCount can have a significant impact on end-to-end query time.
                    Each round trip to the server will return no more than the number of items in MaxItemCount
                    (Default of 100 items). Setting this to a higher value (-1 is maximum, and recommended) will
                    improve your query duration overall by limiting the number of round trips between server and
                    client, especially for queries with large result sets.
                    https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-query-metrics#max-item-count
                */
                MaxItemCount = -1
            };
        }
        
        
        public async Task<TEntity> Get(string id)
        {
            try
            {
                return (await _container.ReadItemAsync<TEntity>(id, _partitionKey)).Resource;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public async Task<bool> Exists(string id)
        {
            return await Get(id) != null;
        }

        public Task Update(TEntity entity)
        {
            return _container.ReplaceItemAsync(entity, entity.Id, _partitionKey);
        }

        public Task Save(TEntity entity)
        {
            return _container.CreateItemAsync(entity, _partitionKey);
        }

        public async Task<bool> Delete(string id)
        {
            try
            {
                await _container.DeleteItemAsync<TEntity>(id, _partitionKey);
                return true;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
        
        public Task<int> Count()
        {
            return Task.FromResult(_container.GetItemLinqQueryable<TEntity>(true, null, _queryRequestOptions)
                .Count(x => x.Type == _type && x.InstanceName == _instanceName));
        }
        
        public Task<IList<TEntity>> GetAll()
        {
            return Task.FromResult((IList<TEntity>)_container.GetItemLinqQueryable<TEntity>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .AsEnumerable()
                .ToList());
        }
        
        public async Task DeleteAll()
        {
            var query = _container.GetItemLinqQueryable<TEntity>(true, null, _queryRequestOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Id)
                .AsEnumerable();

            foreach (var id in query)
            {
                await Delete(id);
            }
        }
    }
}

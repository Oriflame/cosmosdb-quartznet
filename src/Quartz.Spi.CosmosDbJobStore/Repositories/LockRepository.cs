using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    public class LockRepository : CosmosDbRepositoryBase<PersistentLock>
    {
        public LockRepository(IDocumentClient documentClient, string databaseId, string collectionId, string instanceName) 
            : base(documentClient, databaseId, collectionId, PersistentLock.EntityType, instanceName)
        {
        }

        public async Task<bool> TrySave(PersistentLock lck)
        {
            try
            {
                await _documentClient.CreateDocumentAsync(_collectionUri, lck, CreateRequestOptions(), true);
                return true;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.Conflict)
            {
                return false;
            }
        }

        public async Task<bool> TryDelete(string lockId)
        {
            try
            {
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(lockId), CreateRequestOptions());
                return true;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
        
        public Task<IList<PersistentLock>> GetAllByInstanceId(string instanceId)
        {
            return Task.FromResult<IList<PersistentLock>>(_documentClient
                .CreateDocumentQuery<PersistentLock>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.InstanceId == instanceId)
                .AsEnumerable()
                .ToList());
        }
    }
}
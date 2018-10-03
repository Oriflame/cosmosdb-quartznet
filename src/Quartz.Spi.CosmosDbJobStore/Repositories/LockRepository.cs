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
                await _documentClient.CreateDocumentAsync(_collectionUri, lck, disableAutomaticIdGeneration: true);
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
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(lockId));
                return true;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
    }
}
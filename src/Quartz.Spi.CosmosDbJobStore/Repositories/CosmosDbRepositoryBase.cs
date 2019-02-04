using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Quartz.Impl.AdoJobStore;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    public abstract class CosmosDbRepositoryBase<TEntity> where TEntity : QuartzEntityBase
    {
        protected static readonly ILog _logger = LogManager.GetLogger<CosmosDbRepositoryBase<TEntity>>();
        
        protected readonly string _type;
        protected readonly IDocumentClient _documentClient;
        protected readonly string _databaseId;
        protected readonly string _collectionId;
        protected readonly Uri _collectionUri;
        protected readonly string _instanceName;


        protected CosmosDbRepositoryBase(IDocumentClient documentClient, string databaseId, string collectionId, string type, string instanceName)
        {
            _instanceName = instanceName;
            _documentClient = documentClient;
            _databaseId = databaseId;
            _collectionId = collectionId;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            _type = type;
        }


        public async Task EnsureInitialized()
        {
            try
            {
                await _documentClient.ReadDocumentCollectionAsync(_collectionUri);
            }
            catch (DocumentClientException e)
            {
                if (e.StatusCode != HttpStatusCode.NotFound)
                {
                    throw;
                }

                var collection = new DocumentCollection
                {
                    Id = _collectionId,
                    DefaultTimeToLive = -1,
                    IndexingPolicy =
                    {
                        IndexingMode = IndexingMode.Consistent,
                        Automatic = true,
                        IncludedPaths =
                        {
                            new IncludedPath()
                            {
                                Path = "/*",
                                Indexes =
                                {
                                    new RangeIndex(DataType.Number, -1),
                                    new RangeIndex(DataType.String, -1) // Necessary for OrderBy
                                }
                            },
                        }
                    },
                };

                try
                {
                    await _documentClient.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(_databaseId),
                        collection,
                        new RequestOptions { OfferThroughput = 400 });
                }
                catch (DocumentClientException dce)
                {
                    _logger.Warn($"Creation of {GetType().Name} collection failed, it might have been created by another instance.", dce);
                }
            }            
        }
        
        
        public async Task<TEntity> Get(string id)
        {
            try
            {
                return (await _documentClient.ReadDocumentAsync<TEntity>(CreateDocumentUri(id))).Document;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }
        
        public Task<bool> Exists(string id)
        {
            return Task.FromResult(_documentClient
                .CreateDocumentQuery<TEntity>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.Id == id)
                .Take(1)
                .AsEnumerable()
                .Any());
        }

        public Task Update(TEntity entity)
        {
            return _documentClient.UpsertDocumentAsync(_collectionUri, entity, null, true);
        }

        public Task Save(TEntity entity)
        {
            return _documentClient.CreateDocumentAsync(_collectionUri, entity, null, true);
        }

        public async Task<bool> Delete(string id)
        {
            try
            {
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(id));
                return true;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
        
        public Task<int> Count()
        {
            return Task.FromResult(_documentClient
                .CreateDocumentQuery<TEntity>(_collectionUri)
                .Count(x => x.Type == _type && x.InstanceName == _instanceName));
        }
        
        public Task<IList<TEntity>> GetAll()
        {
            return Task.FromResult((IList<TEntity>)_documentClient.CreateDocumentQuery<TEntity>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .AsEnumerable()
                .ToList());
        }
        
        public async Task DeleteAll()
        {
            var all = _documentClient.CreateDocumentQuery<TEntity>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Id)
                .AsEnumerable()
                .ToList();

            foreach (var id in all)
            {
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(id));
            }
        }
        
        protected Uri CreateDocumentUri(string id)
        {
            return UriFactory.CreateDocumentUri(_databaseId, _collectionId, id);
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
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
        private readonly string _partitionKeyPath;

        protected CosmosDbRepositoryBase(IDocumentClient documentClient,
                                         string databaseId,
                                         string collectionId,
                                         string type,
                                         string instanceName,
                                         bool partitionPerEntityType)
        {
            _instanceName = instanceName;
            _documentClient = documentClient;
            _databaseId = databaseId;
            _collectionId = collectionId;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(databaseId, collectionId);
            _type = type;
            _partitionKeyPath = partitionPerEntityType ? "/type" : "/instanceName";

            var partitionKey = new PartitionKey(partitionPerEntityType ? type : instanceName);

            RequestOptions = new RequestOptions { PartitionKey = partitionKey };

            FeedOptions = new FeedOptions
            {
                PartitionKey = partitionKey,
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
                            new IncludedPath
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
                    PartitionKey = new PartitionKeyDefinition { Paths = { _partitionKeyPath } }
                };

                try
                {
                    await _documentClient.CreateDocumentCollectionAsync(
                        UriFactory.CreateDatabaseUri(_databaseId),
                        collection);
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
                return (await _documentClient.ReadDocumentAsync<TEntity>(CreateDocumentUri(id), RequestOptions)).Document;
            }
            catch (DocumentClientException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        

        public Task<bool> Exists(string id)
        {
            return Task.FromResult(_documentClient
                .CreateDocumentQuery<TEntity>(_collectionUri, FeedOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.Id == id)
                .Take(1)
                .AsEnumerable()
                .Any());
        }

        public Task Update(TEntity entity)
        {
            return _documentClient.UpsertDocumentAsync(_collectionUri, entity, RequestOptions, true);
        }

        public Task Save(TEntity entity)
        {
            return _documentClient.CreateDocumentAsync(_collectionUri, entity, RequestOptions, true);
        }

        public async Task<bool> Delete(string id)
        {
            try
            {
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(id), RequestOptions);
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
                .CreateDocumentQuery<TEntity>(_collectionUri, FeedOptions)
                .Count(x => x.Type == _type && x.InstanceName == _instanceName));
        }
        
        public Task<IList<TEntity>> GetAll()
        {
            return Task.FromResult((IList<TEntity>)_documentClient.CreateDocumentQuery<TEntity>(_collectionUri, FeedOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .AsEnumerable()
                .ToList());
        }
        
        public async Task DeleteAll()
        {
            var all = _documentClient.CreateDocumentQuery<TEntity>(_collectionUri, FeedOptions)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Id)
                .AsEnumerable()
                .ToList();

            var requestOptions = RequestOptions;
            
            foreach (var id in all)
            {
                await _documentClient.DeleteDocumentAsync(CreateDocumentUri(id), requestOptions);
            }
        }
        
        protected Uri CreateDocumentUri(string id)
        {
            return UriFactory.CreateDocumentUri(_databaseId, _collectionId, id);
        }

        protected RequestOptions RequestOptions { get; }

        protected FeedOptions FeedOptions { get; }
    }
}

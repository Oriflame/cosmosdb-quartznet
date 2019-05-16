using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class CalendarRepository : CosmosDbRepositoryBase<PersistentCalendar>
    {
        public CalendarRepository(IDocumentClient documentClient, string databaseId, string collectionId, string instanceName)
            : base(documentClient, databaseId, collectionId, PersistentCalendar.EntityType, instanceName)
        {
        }


        public Task<IReadOnlyCollection<string>> GetCalendarNames()
        {
            return Task.FromResult((IReadOnlyCollection<string>)_documentClient
                .CreateDocumentQuery<PersistentCalendar>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.CalendarName)
                .AsEnumerable()
                .Distinct()
                .ToList());
        } 
    }
}
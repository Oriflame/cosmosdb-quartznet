using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Quartz.Spi.CosmosDbJobStore.Entities;

namespace Quartz.Spi.CosmosDbJobStore.Repositories
{
    internal class TriggerRepository : CosmosDbRepositoryBase<PersistentTriggerBase>
    {
        public TriggerRepository(IDocumentClient documentClient, string databaseId, string collectionId, string instanceName)
            : base(documentClient, databaseId, collectionId, PersistentTriggerBase.EntityType, instanceName)
        {
        }

        
        public Task<IList<PersistentTriggerBase>> GetAllByJob(string jobName, string jobGroup)
        {
            return Task.FromResult<IList<PersistentTriggerBase>>(_documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName)
                .AsEnumerable()
                .ToList());
        }
        
        public Task<IList<PersistentTriggerBase>> GetAllByJobAndState(JobKey jobKey, PersistentTriggerState state)
        {
            var jobName = jobKey?.Name;
            var jobGroup = jobKey?.Group;
            
            return Task.FromResult<IList<PersistentTriggerBase>>(_documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName && x.State == state)
                .AsEnumerable()
                .ToList());
        }
        
        public Task<int> CountByJob(string jobName, string jobGroup)
        {
            return Task.FromResult(_documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Count(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName));
        }
        
        public Task<IList<PersistentTriggerBase>> GetAllCompleteByCalendar(string calendarName)
        {
            return Task.FromResult<IList<PersistentTriggerBase>>(_documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.CalendarName == calendarName)
                .AsEnumerable()
                .ToList());
        }

        public Task<bool> ExistsByCalendar(string calendarName)
        {
            return Task.FromResult(_documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.CalendarName == calendarName)
                .Take(1)
                .AsEnumerable()
                .Any());
        }
        
        public Task<int> GetMisfireCount(DateTimeOffset nextFireTime)
        {
            return Task.FromResult(_documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Count(x => x.Type == _type && x.InstanceName == _instanceName && x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy && x.NextFireTime < nextFireTime && x.State == PersistentTriggerState.Waiting));
        }
        
        /// <summary>
        /// Get the names of all of the triggers in the given state that have
        /// misfired - according to the given timestamp.  No more than count will be returned.
        /// </summary>
        /// <param name="nextFireTime"></param>
        /// <param name="limit"></param>
        /// <returns></returns>
        public Task<IList<PersistentTriggerBase>> GetAllMisfired(DateTimeOffset nextFireTime, int limit)
        {
            return Task.FromResult<IList<PersistentTriggerBase>>(_documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy && x.NextFireTime < nextFireTime && x.State == PersistentTriggerState.Waiting)
                .OrderBy(x => x.NextFireTime) // Note that Cosmos can't do it at the moment :-( .ThenByDescending(x => x.Priority)
                .Take(limit)
                .AsEnumerable()
                .ToList());
        }
        
        public Task<List<PersistentTriggerBase>> GetAllByState(params PersistentTriggerState[] states)
        {
            return Task.FromResult<List<PersistentTriggerBase>>(_documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && states.Contains(x.State))
                .AsEnumerable()
                .ToList());
        }

        public async Task<int> UpdateAllByStates(PersistentTriggerState newState, params PersistentTriggerState[] oldStates)
        {
            var triggers = await GetAllByState(oldStates);

            foreach (var trigger in triggers)
            {
                trigger.State = newState;
                await _documentClient.UpsertDocumentAsync(CreateDocumentUri(trigger.Id), trigger, CreateRequestOptions(), true);
            }
            return triggers.Count;
        }

        public Task<IReadOnlyCollection<string>> GetGroups()
        {
            return Task.FromResult<IReadOnlyCollection<string>>(_documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .AsEnumerable()
                .Distinct()
                .ToList());
        }
        
        public Task<List<PersistentTriggerBase>> GetAllToAcquire(DateTimeOffset noLaterThan, DateTimeOffset noEarlierThan, int maxCount)
        {
            if (maxCount < 1)
            {
                maxCount = 1;
            }

            return Task.FromResult<List<PersistentTriggerBase>>(_documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri, CreateFeedOptions())
                .Where(x => x.Type == _type && x.InstanceName == _instanceName 
                                            && x.State == PersistentTriggerState.Waiting && x.NextFireTime <= noLaterThan 
                                            && (x.MisfireInstruction == -1 || (x.MisfireInstruction != -1 && x.NextFireTime >= noEarlierThan)))
                .OrderBy(x => x.NextFireTime) // Cosmos can't do this! .ThenByDescending(x => x.Priority)
                .Take(maxCount)
                .AsEnumerable()
                .ToList());
        }
    }
}
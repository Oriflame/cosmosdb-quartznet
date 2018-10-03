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

        
        public async Task<IList<PersistentTriggerBase>> GetAllByJob(string jobName, string jobGroup)
        {
            return _documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName)
                .AsEnumerable()
                .ToList();
        }
        
        public async Task<IList<PersistentTriggerBase>> GetAllByJobAndState(JobKey jobKey, PersistentTriggerState state)
        {
            return _documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobKey.Group && x.JobName == jobKey.Name && x.State == state)
                .AsEnumerable()
                .ToList();
        }
        
        public async Task<int> CountByJob(string jobName, string jobGroup)
        {
            return _documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Count(x => x.Type == _type && x.InstanceName == _instanceName && x.JobGroup == jobGroup && x.JobName == jobName);
        }
        
        public async Task<IList<PersistentTriggerBase>> GetAllCompleteByCalendar(string calendarName)
        {
            return _documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.CalendarName == calendarName)
                .AsEnumerable()
                .ToList();
        }

        public async Task<bool> ExistsByCalendar(string calendarName)
        {
            return _documentClient
                .CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.CalendarName == calendarName)
                .Take(1)
                .AsEnumerable()
                .Any();
        }
        
        public async Task<int> GetMisfireCount(DateTimeOffset nextFireTime)
        {
            return _documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Count(x => x.Type == _type && x.InstanceName == _instanceName && x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy && x.NextFireTime < nextFireTime && x.State == PersistentTriggerState.Waiting);
        }
        
        /// <summary>
        /// Get the names of all of the triggers in the given state that have
        /// misfired - according to the given timestamp.  No more than count will be returned.
        /// </summary>
        /// <param name="nextFireTime"></param>
        /// <param name="maxResults"></param>
        /// <param name="results"></param>
        /// <returns></returns>
        public async Task<IList<PersistentTriggerBase>> GetAllMisfired(DateTimeOffset nextFireTime, int limit)
        {
            return _documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy && x.NextFireTime < nextFireTime && x.State == PersistentTriggerState.Waiting)
                .OrderBy(x => x.NextFireTime) // Note that Cosmos can't do it at the moment :-( .ThenByDescending(x => x.Priority)
                .Take(limit)
                .AsEnumerable()
                .ToList();
        }
        
        public async Task<List<PersistentTriggerBase>> GetAllByState(params PersistentTriggerState[] states)
        {
            return _documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName && states.Contains(x.State))
                .AsEnumerable()
                .ToList();
        }

        public async Task<int> UpdateAllByStates(PersistentTriggerState newState, params PersistentTriggerState[] oldStates)
        {
            var triggers = await GetAllByState(oldStates);

            foreach (var trigger in triggers)
            {
                trigger.State = newState;
                await _documentClient.UpsertDocumentAsync(CreateDocumentUri(trigger.Id), trigger);
            }
            return triggers.Count;
        }

        public async Task<IReadOnlyCollection<string>> GetGroups()
        {
            return _documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName)
                .Select(x => x.Group)
                .AsEnumerable()
                .Distinct()
                .ToList();
        }
        
        public async Task<List<PersistentTriggerBase>> GetAllToAcquire(DateTimeOffset noLaterThan, DateTimeOffset noEarlierThan, int maxCount)
        {
            if (maxCount < 1)
            {
                maxCount = 1;
            }

            return _documentClient.CreateDocumentQuery<PersistentTriggerBase>(_collectionUri)
                .Where(x => x.Type == _type && x.InstanceName == _instanceName 
                                            && x.State == PersistentTriggerState.Waiting && x.NextFireTime <= noLaterThan 
                                            && (x.MisfireInstruction == -1 || (x.MisfireInstruction != -1 && x.NextFireTime >= noEarlierThan)))
                .OrderBy(x => x.NextFireTime) // Cosmos can't do this! .ThenByDescending(x => x.Priority)
                .Take(maxCount)
                .AsEnumerable()
                .ToList();
        }
    }
}
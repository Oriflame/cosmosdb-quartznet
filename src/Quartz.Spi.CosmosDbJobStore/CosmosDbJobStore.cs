using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using Microsoft.Azure.Documents.Client;
using Quartz.Impl.AdoJobStore;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Spi.CosmosDbJobStore.Entities;
using Quartz.Spi.CosmosDbJobStore.Repositories;
using Quartz.Spi.CosmosDbJobStore.Util;

namespace Quartz.Spi.CosmosDbJobStore
{
    public class CosmosDbJobStore : IJobStore
    {
        private const string KeySignalChangeForTxCompletion = "sigChangeForTxCompletion";
        private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";

        private static readonly ILog _logger = LogManager.GetLogger<CosmosDbJobStore>();
        
        public static readonly DateTimeOffset? SchedulingSignalDateTime = new DateTimeOffset(1982, 6, 28, 0, 0, 0, TimeSpan.FromSeconds(0));
        
        private static long _fireTriggerRecordCounter = DateTimeOffset.UtcNow.Ticks;
        
        private TimeSpan _misfireThreshold = TimeSpan.FromMinutes(1);
        private bool _schedulerRunning;
        private ISchedulerSignaler _schedulerSignaler;
        private MisfireHandler _misfireHandler;
        private LockManager _lockManager;
        private TriggerRepository _triggerRepository;
        private CalendarRepository _calendarRepository;
        private JobRepository _jobRepository;
        private SchedulerRepository _schedulerRepository;
        private FiredTriggerRepository _firedTriggerRepository;
        private PausedTriggerGroupRepository _pausedTriggerGroupRepository;
        private ClusterManager _clusterManager;
        
        
        /// <summary>
        /// The time span by which a trigger must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// have its misfire instruction applied.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan MisfireThreshold
        {
            get => _misfireThreshold;
            set
            {
                if (value.TotalMilliseconds < 1)
                {
                    throw new ArgumentException("MisfireThreshold must be larger than 0");
                }

                _misfireThreshold = value;
            }
        }
 
        /// <summary>
        ///     Gets or sets the database retry interval.
        /// </summary>
        /// <value>The db retry interval.</value>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan DbRetryInterval { get; set; }
        
        /// <summary>
        /// Get or set the frequency at which this instance "checks-in"
        /// with the other instances of the cluster. -- Affects the rate of
        /// detecting failed instances.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan ClusterCheckinInterval { get; set; }
        
        /// <summary>
        /// The time span by which a check-in must have missed its
        /// next-fire-time, in order for it to be considered "misfired" and thus
        /// other scheduler instances in a cluster can consider a "misfired" scheduler
        /// instance as failed or dead.
        /// </summary>
        [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
        public TimeSpan ClusterCheckinMisfireThreshold { get; set; }

        /// <summary>
        /// After specified seconds, the lock will be deleted.
        /// </summary>
        public int LockTtlSeconds { get; set; }
        
        /// <summary>
        ///     Gets or sets the number of retries before an error is logged for recovery operations.
        /// </summary>
        public int RetryableActionErrorLogThreshold { get; set; }
        
        public bool SupportsPersistence { get; } = true;
        
        public long EstimatedTimeToReleaseAndAcquireTrigger { get; }
        
        public bool Clustered { get; set; }
        
        public string InstanceId { get; set; }
        
        public string InstanceName { get; set; }
        
        public int ThreadPoolSize { get; set; }

        /// <summary>
        /// CosmosDB endpoint
        /// </summary>
        public string Endpoint { get; set; }
        
        /// <summary>
        /// CosmosDB auth. key
        /// </summary>
        public string Key { get; set; }
        
        /// <summary>
        /// CosmosDB DatabaseId
        /// </summary>
        public string DatabaseId { get; set; }
        
        /// <summary>
        /// CosmosDB CollectionId
        /// </summary>
        public string CollectionId { get; set; }
        
        /// <summary>
        /// Get whether the threads spawned by this JobStore should be
        /// marked as daemon.  Possible threads include the <see cref="MisfireHandler" />
        /// and the <see cref="ClusterManager"/>.
        /// </summary>
        /// <returns></returns>
        public bool MakeThreadsDaemons { get; set; }

        /// <summary>
        ///     Get or set the maximum number of misfired triggers that the misfire handling
        ///     thread will try to recover at one time (within one transaction).  The
        ///     default is 20.
        /// </summary>
        public int MaxMisfiresToHandleAtATime { get; set; }

        protected DateTimeOffset MisfireTime
        {
            get
            {
                var misfireTime = SystemTime.UtcNow();
                if (MisfireThreshold > TimeSpan.Zero)
                {
                    misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
                }

                return misfireTime;
            }
        }

        
        public CosmosDbJobStore()
        {
            ClusterCheckinInterval = TimeSpan.FromMilliseconds(7500);
            ClusterCheckinMisfireThreshold = TimeSpan.FromMilliseconds(52500); // Failover will start after 1 minute by default
            MaxMisfiresToHandleAtATime = 20;
            RetryableActionErrorLogThreshold = 4;
            DbRetryInterval = TimeSpan.FromSeconds(5);
            LockTtlSeconds = 10 * 60; // 10 minutes
        }
        
        
        public async Task Initialize(ITypeLoadHelper loadHelper, ISchedulerSignaler signaler,
            CancellationToken cancellationToken = new CancellationToken())
        {           
            _schedulerSignaler = signaler;

            var serializerSettings = new InheritedJsonObjectSerializer();
            var documentClient = new DocumentClient(new Uri(Endpoint), Key, serializerSettings.CreateSerializerSettings(), new ConnectionPolicy
            {
                ConnectionMode = ConnectionMode.Direct,
                ConnectionProtocol = Protocol.Tcp,
                RequestTimeout = TimeSpan.FromSeconds(30),
                MaxConnectionLimit = 10,
                RetryOptions = new RetryOptions
                {
                    MaxRetryWaitTimeInSeconds = 3,
                    MaxRetryAttemptsOnThrottledRequests = 10
                }
            }); // TODO Configurable
            
            _lockManager = new LockManager(new LockRepository(documentClient, DatabaseId, CollectionId, InstanceName), InstanceName, InstanceId, LockTtlSeconds);
            _calendarRepository = new CalendarRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            _triggerRepository = new TriggerRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            _jobRepository = new JobRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            _schedulerRepository = new SchedulerRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            _firedTriggerRepository = new FiredTriggerRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            _pausedTriggerGroupRepository = new PausedTriggerGroupRepository(documentClient, DatabaseId, CollectionId, InstanceName);
            await _schedulerRepository.EnsureInitialized(); // All repositories uses one collection
        }
        

        public async Task SchedulerStarted(CancellationToken cancellationToken = new CancellationToken())
        {
            _logger.Trace($"Scheduler {InstanceName} / {InstanceId} started");
            
            if (Clustered)
            {
                _clusterManager = new ClusterManager(this);
                await _clusterManager.Initialize().ConfigureAwait(false);
            }
            else
            {
                try
                {
                    await RecoverJobs();
                }
                catch (Exception e)
                {
                    _logger.Error($"Failure occurred during job recovery: {e.Message}", e);
                    throw new SchedulerConfigException("Failure occurred during job recovery", e);
                }
            }
           
            await _schedulerRepository.Update(new PersistentScheduler(InstanceName, InstanceId)
            {
                LastCheckIn = DateTimeOffset.UtcNow
            });           

            _misfireHandler = new MisfireHandler(this);
            _misfireHandler.Start();
            _schedulerRunning = true;
        }

        public Task SchedulerPaused(CancellationToken cancellationToken = new CancellationToken())
        {
            _logger.Trace($"Scheduler {InstanceName} / {InstanceId} paused");

            _schedulerRunning = false;
            
            return Task.CompletedTask;
        }

        public Task SchedulerResumed(CancellationToken cancellationToken = new CancellationToken())
        {
            _logger.Trace($"Scheduler {InstanceName} / {InstanceId} resumed");

            _schedulerRunning = true;
            
            return Task.CompletedTask;
        }

        public async Task Shutdown(CancellationToken cancellationToken = new CancellationToken())
        {
            _logger.Trace($"Scheduler {InstanceName} / {InstanceId} shutdown");
            
            if (_misfireHandler != null)
            {
                _misfireHandler.Shutdown();
                try
                {
                    _misfireHandler.Join();
                }
                catch (ThreadInterruptedException)
                {
                }
            }
            
            if (_clusterManager != null)
            {
                await _clusterManager.Shutdown();
            }

            await _schedulerRepository.Delete(PersistentScheduler.GetId(InstanceName, InstanceId));
            
            _lockManager.Dispose();
        }

        public async Task StoreJobAndTrigger(IJobDetail newJob, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    await StoreJobInternal(newJob, false);
                    await StoreTriggerInternal(newTrigger, newJob, false, PersistentTriggerState.Waiting, false, false, cancellationToken);
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<bool> IsJobGroupPaused(string groupName, CancellationToken cancellationToken = new CancellationToken())
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken cancellationToken = new CancellationToken())
        {
            // This is not implemented in the core ADO stuff, so we won't implement it here either
            throw new NotImplementedException();
        }

        public async Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    await StoreJobInternal(newJob, replaceExisting);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task StoreJobsAndTriggers(IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs, bool replace,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    foreach (var job in triggersAndJobs.Keys)
                    {
                        await StoreJobInternal(job, replace);
                        foreach (var trigger in triggersAndJobs[job])
                            await StoreTriggerInternal((IOperableTrigger) trigger, job, replace, PersistentTriggerState.Waiting, false, false, cancellationToken);
                    }
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    return await RemoveJobWithTriggersInternal(jobKey.Name, jobKey.Group);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                var removed = false;
                
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    foreach (var jobKey in jobKeys)
                    {
                        removed |= await RemoveJobWithTriggersInternal(jobKey.Name, jobKey.Group);
                    }

                    return removed;
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IJobDetail> RetrieveJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var result = await _jobRepository.Get(PersistentJob.GetId(InstanceName, jobKey));
            return result?.GetJobDetail();
        }

        public async Task StoreTrigger(IOperableTrigger newTrigger, bool replaceExisting, CancellationToken cancellationToken = new CancellationToken())
        {
            using (await _lockManager.AcquireLock(LockType.TriggerAccess))
            {
                await StoreTriggerInternal(newTrigger, null, replaceExisting, PersistentTriggerState.Waiting, false, false, cancellationToken);
            }
        }

        public async Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
                    if (trigger == null)
                    {
                        return false;
                    }
                    
                    return await RemoveTriggerInternal(trigger);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                var removed = false;
                
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    foreach (var triggerKey in triggerKeys)
                    {
                        var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
                        if (trigger == null)
                        {
                            continue;
                        }
                        
                        removed |= await RemoveTriggerInternal(trigger);
                    }

                    return removed;
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> ReplaceTrigger(TriggerKey triggerKey, IOperableTrigger newTrigger,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
                    if (trigger == null)
                    {
                        return false;
                    }
                    var result = await _jobRepository.Get(PersistentJob.GetId(InstanceName, trigger.JobGroup, trigger.JobName));
                    var job = result?.GetJobDetail();

                    if (job == null)
                    {
                        return false;
                    }

                    if (!newTrigger.JobKey.Equals(job.Key))
                    {
                        throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
                    }

                    var removedTrigger = await _triggerRepository.Delete(trigger.Id);
                    await StoreTriggerInternal(newTrigger, job, false, PersistentTriggerState.Waiting, false, false, cancellationToken);
                    
                    return removedTrigger;
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IOperableTrigger> RetrieveTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var result = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
            return result?.GetTrigger() as IOperableTrigger;
        }

        public Task<bool> CalendarExists(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            return _calendarRepository.Exists(PersistentCalendar.GetId(InstanceName, calName));
        }

        public Task<bool> CheckExists(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return _jobRepository.Exists(PersistentJob.GetId(InstanceName, jobKey));
        }

        public Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            return _triggerRepository.Exists(PersistentTriggerBase.GetId(InstanceName, triggerKey));
        }

        public async Task ClearAllSchedulingData(CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    await _calendarRepository.DeleteAll();
                    await _firedTriggerRepository.DeleteAll();
                    await _jobRepository.DeleteAll();
                    await _pausedTriggerGroupRepository.DeleteAll();
                    await _schedulerRepository.DeleteAll();
                    await _triggerRepository.DeleteAll();
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task StoreCalendar(string calName, ICalendar calendar, bool replaceExisting, bool updateTriggers,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var existingCal = await CalendarExists(calName, cancellationToken);
                    if (existingCal && !replaceExisting)
                    {
                        throw new ObjectAlreadyExistsException($"Calendar {calName} already exists.");
                    }

                    var persistentCalendar = new PersistentCalendar(InstanceName, calName, calendar);
                    
                    if (!existingCal)
                    {
                        await _calendarRepository.Save(persistentCalendar);
                        return;
                    }

                    await _calendarRepository.Update(persistentCalendar);

                    if (updateTriggers)
                    {
                        var triggers = await _triggerRepository.GetAllCompleteByCalendar(calName);
                        foreach (var trigger in triggers)
                        {
                            var quartzTrigger = (IOperableTrigger) trigger.GetTrigger();
                            quartzTrigger.UpdateWithNewCalendar(calendar, MisfireThreshold);
                            await StoreTriggerInternal(quartzTrigger, null, true, PersistentTriggerState.Waiting, false, false, cancellationToken);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<bool> RemoveCalendar(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    if (await _triggerRepository.ExistsByCalendar(calName))
                    {
                        throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
                    }

                    return await _calendarRepository.Delete(PersistentCalendar.GetId(InstanceName, calName));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        private async Task StoreJobInternal(IJobDetail newJob, bool replaceExisting)
        {
            var existingJob = await _jobRepository.Exists(PersistentJob.GetId(InstanceName, newJob.Key));

            if (existingJob)
            {
                if (!replaceExisting)
                {
                    throw new ObjectAlreadyExistsException(newJob);
                }

                await _jobRepository.Update(new PersistentJob(newJob, InstanceName));
            }
            else
            {
                await _jobRepository.Save(new PersistentJob(newJob, InstanceName));
            }
        }
        
        public async Task<ICalendar> RetrieveCalendar(string calName, CancellationToken cancellationToken = new CancellationToken())
        {
            return (await _calendarRepository.Get(PersistentCalendar.GetId(InstanceName, calName)))?.Calendar;
        }
        
        private async Task StoreTriggerInternal(IOperableTrigger newTrigger, IJobDetail job, bool replaceExisting, PersistentTriggerState state, bool forceState, bool recovering, CancellationToken token = default)
        {
            var existingTrigger = await _triggerRepository.Exists(PersistentTriggerBase.GetId(InstanceName, newTrigger.Key));

            if (existingTrigger && !replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newTrigger);
            }

            if (!forceState)
            {
                var shouldBePaused = await _pausedTriggerGroupRepository.Exists(PausedTriggerGroup.GetId(InstanceName, newTrigger.Key.Group));

                if (!shouldBePaused)
                {
                    shouldBePaused = await _pausedTriggerGroupRepository.Exists(PausedTriggerGroup.GetId(InstanceName, AllGroupsPaused));
                    
                    if (shouldBePaused)
                    {
                        await _pausedTriggerGroupRepository.Save(new PausedTriggerGroup(newTrigger.Key.Group, InstanceName));
                    }
                }

                if (shouldBePaused &&
                    state == PersistentTriggerState.Waiting || state == PersistentTriggerState.Acquired)
                {
                    state = PersistentTriggerState.Paused;
                }
            }

            if (job == null)
            {
                job = (await _jobRepository.Get(PersistentJob.GetId(InstanceName, newTrigger.JobKey)))?.GetJobDetail();
            }

            if (job == null)
            {
                throw new JobPersistenceException($"The job ({newTrigger.JobKey}) referenced by the trigger does not exist.");
            }

            if (job.ConcurrentExecutionDisallowed && !recovering)
            {
                state = await CheckBlockedState(job.Key, state);
            }

            if (existingTrigger)
            {
                await _triggerRepository.Update(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName));
            }
            else
            {
                await _triggerRepository.Save(TriggerFactory.CreateTrigger(newTrigger, state, InstanceName));
            }
        }

        public Task<int> GetNumberOfJobs(CancellationToken cancellationToken = new CancellationToken())
        {
            return _jobRepository.Count();
        }

        public Task<int> GetNumberOfTriggers(CancellationToken cancellationToken = new CancellationToken())
        {
            return _triggerRepository.Count();
        }

        public async Task<int> GetNumberOfCalendars(CancellationToken cancellationToken = new CancellationToken())
        {
            return await _calendarRepository.Count();
        }

        public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            // This is not very optimal implementation..
            return (await _jobRepository.GetAll()).Select(x => x.GetJobKey()).Where(matcher.IsMatch).ToList();
        }

        public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            // This is not very optimal implementation..
            return (await _triggerRepository.GetAll()).Select(x => x.GetTriggerKey()).Where(matcher.IsMatch).ToList();
        }

        public Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return _jobRepository.GetGroups();
        }

        public Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return _triggerRepository.GetGroups();
        }

        public Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken cancellationToken = new CancellationToken())
        {
            return _calendarRepository.GetCalendarNames();
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var result = await _triggerRepository.GetAllByJob(jobKey.Name, jobKey.Group);
            return result.Select(trigger => trigger.GetTrigger() as IOperableTrigger).ToList();
        }

        public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));

            if (trigger == null)
            {
                return TriggerState.None;
            }

            switch (trigger.State)
            {
                case PersistentTriggerState.None:
                case PersistentTriggerState.Deleted:
                    return TriggerState.None;
                case PersistentTriggerState.Complete:
                    return TriggerState.Complete;
                case PersistentTriggerState.Paused:
                case PersistentTriggerState.PausedBlocked:
                    return TriggerState.Paused;
                case PersistentTriggerState.Error:
                    return TriggerState.Error;
                case PersistentTriggerState.Blocked:
                    return TriggerState.Blocked;
                default:
                    return TriggerState.Normal;
            }
        }

        public async Task PauseTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    await PauseTriggerInternal(triggerKey.Name, triggerKey.Group);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    return await PauseTriggerGroupInternal(matcher);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        private async Task<IReadOnlyCollection<string>> PauseTriggerGroupInternal(GroupMatcher<TriggerKey> matcher)
        {
            var triggers = (await _triggerRepository.GetAll()).Where(x => matcher.IsMatch(x.GetTriggerKey()));

            foreach (var trigger in triggers)
            {
                await PauseTriggerInternal(trigger.Name, trigger.Group);
            }

            var triggerGroups = (await _triggerRepository.GetGroups()).Where(x => matcher.IsMatch(new TriggerKey("not_used", x))).ToList();

            // make sure to account for an exact group match for a group that doesn't yet exist
            var op = matcher.CompareWithOperator;
            if (op.Equals(StringOperator.Equality) && !triggerGroups.Contains(matcher.CompareToValue))
            {
                triggerGroups.Add(matcher.CompareToValue);
            }

            foreach (var triggerGroup in triggerGroups)
                if (!await _pausedTriggerGroupRepository.Exists(PausedTriggerGroup.GetId(InstanceName, triggerGroup)))
                {
                    await _pausedTriggerGroupRepository.Save(new PausedTriggerGroup(triggerGroup, InstanceName));
                }

            return new HashSet<string>(triggerGroups);
        }

        public async Task PauseJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var triggers = await GetTriggersForJob(jobKey, cancellationToken);
                    foreach (var operableTrigger in triggers)
                        await PauseTriggerInternal(operableTrigger.Key.Name, operableTrigger.Key.Group);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var jobs = (await _jobRepository.GetAll()).Where(x => matcher.IsMatch(x.GetJobKey())).ToList(); // This is again not very optimal
                    foreach (var jobKey in jobs)
                    {
                        var triggers = await _triggerRepository.GetAllByJob(jobKey.Name, jobKey.Group);
                        foreach (var trigger in triggers)
                        {
                            await PauseTriggerInternal(trigger.Name, trigger.Group);
                        }
                    }

                    return jobs.Select(key => key.Group).Distinct().ToList();
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ResumeTrigger(TriggerKey triggerKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
                    await ResumeTriggerInternal(trigger);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeTriggers(GroupMatcher<TriggerKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    return await ResumeTriggersInternal(matcher);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = new CancellationToken())
        {
            return _pausedTriggerGroupRepository.GetGroups();
        }

        public async Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var triggers = await _triggerRepository.GetAllByJob(jobKey.Name, jobKey.Group);
                    await Task.WhenAll(triggers.Select(ResumeTriggerInternal));
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<string>> ResumeJobs(GroupMatcher<JobKey> matcher, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var jobs = (await _jobRepository.GetAll()).Where(x => matcher.IsMatch(x.GetJobKey())).ToList(); // This is not very optimal
                    foreach (var job in jobs)
                    {
                        var triggers = await _triggerRepository.GetAllByJob(job.Name, job.Group);
                        await Task.WhenAll(triggers.Select(ResumeTriggerInternal));
                    }

                    return new HashSet<string>(jobs.Select(x => x.Group));
                }
            }
            catch (AggregateException ex)
            {
                throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task PauseAll(CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var groupNames = await _triggerRepository.GetGroups();

                    await Task.WhenAll(groupNames.Select(groupName =>
                        PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName))));

                    if (!await _pausedTriggerGroupRepository.Exists(PausedTriggerGroup.GetId(InstanceName, AllGroupsPaused)))
                    {
                        await _pausedTriggerGroupRepository.Save(new PausedTriggerGroup(AllGroupsPaused, InstanceName));
                    }
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ResumeAll(CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var groupNames = await _triggerRepository.GetGroups();
                    await Task.WhenAll(groupNames.Select(groupName =>
                        ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName))));
                    await _pausedTriggerGroupRepository.Delete(PausedTriggerGroup.GetId(InstanceName, AllGroupsPaused));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(DateTimeOffset noLaterThan, int maxCount, TimeSpan timeWindow,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    return await AcquireNextTriggersInternal(noLaterThan, maxCount, timeWindow);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var persistentTrigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, trigger.Key));

                    if (persistentTrigger == null)
                    {
                        return;
                    }

                    if (persistentTrigger.State == PersistentTriggerState.Acquired)
                    {
                        persistentTrigger.State = PersistentTriggerState.Waiting;
                        await _triggerRepository.Update(persistentTrigger);
                    }
                    
                    await _firedTriggerRepository.Delete(PersistentFiredTrigger.GetId(InstanceName, trigger.FireInstanceId));
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }
       
        public async Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(IReadOnlyCollection<IOperableTrigger> triggers, CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    var results = new List<TriggerFiredResult>();

                    foreach (var operableTrigger in triggers)
                    {
                        TriggerFiredResult result;
                        try
                        {
                            var bundle = await TriggerFiredInternal(operableTrigger);
                            result = new TriggerFiredResult(bundle);
                        }
                        catch (Exception ex)
                        {
                            _logger.Error($"Caught exception: {ex.Message}", ex);
                            result = new TriggerFiredResult(ex);
                        }

                        results.Add(result);
                    }

                    return results;
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        public async Task TriggeredJobComplete(IOperableTrigger trigger, IJobDetail jobDetail, SchedulerInstruction triggerInstCode,
            CancellationToken cancellationToken = new CancellationToken())
        {
            try
            {
                using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                {
                    await TriggeredJobCompleteInternal(trigger, jobDetail, triggerInstCode);
                }

                var sigTime = ClearAndGetSignalSchedulingChangeOnTxCompletion();
                if (sigTime != null)
                {
                    SignalSchedulingChangeImmediately(sigTime);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }

        internal async Task<RecoverMisfiredJobsResult> DoRecoverMisfires()
        {
            try
            {
                var result = RecoverMisfiredJobsResult.NoOp;

                var misfireCount = await _triggerRepository.GetMisfireCount(MisfireTime);
                
                if (misfireCount == 0)
                {
                    _logger.Debug("Found 0 triggers that missed their scheduled fire-time.");
                }
                else
                {
                    using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                    {
                        result = await RecoverMisfiredJobsInternal(false);
                    }
                }

                return result;
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }
        
        internal virtual void SignalSchedulingChangeImmediately(DateTimeOffset? candidateNewNextFireTime)
        {
            _schedulerSignaler.SignalSchedulingChange(candidateNewNextFireTime);
        }
        
        protected virtual DateTimeOffset? ClearAndGetSignalSchedulingChangeOnTxCompletion()
        {
            var t = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
            LogicalThreadContext.FreeNamedDataSlot(KeySignalChangeForTxCompletion);
            return t;
        }
        
        private async Task TriggeredJobCompleteInternal(IOperableTrigger trigger, IJobDetail jobDetail,
            SchedulerInstruction triggerInstCode)
        {
            try
            {
                var persistentTrigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, trigger.Key));
                
                switch (triggerInstCode)
                {
                    case SchedulerInstruction.DeleteTrigger:
                        if (!trigger.GetNextFireTimeUtc().HasValue)
                        {
                            if (persistentTrigger != null && !persistentTrigger.NextFireTime.HasValue)
                            {
                                await RemoveTriggerInternal(persistentTrigger, jobDetail);
                            }
                        }
                        else
                        {
                            await RemoveTriggerInternal(persistentTrigger, jobDetail);
                            SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        }

                        break;
                    case SchedulerInstruction.SetTriggerComplete:
                        persistentTrigger.State = PersistentTriggerState.Complete;
                        await _triggerRepository.Update(persistentTrigger);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetTriggerError:
                        _logger.Info("Trigger " + trigger.Key + " set to ERROR state.");
                        persistentTrigger.State = PersistentTriggerState.Error;
                        await _triggerRepository.Update(persistentTrigger);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersComplete:
                        persistentTrigger.State = PersistentTriggerState.Complete;
                        await _triggerRepository.Update(persistentTrigger);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                    case SchedulerInstruction.SetAllJobTriggersError:
                        _logger.Info("All triggers of Job " + trigger.JobKey + " set to ERROR state.");
                        persistentTrigger.State = PersistentTriggerState.Error;
                        await _triggerRepository.Update(persistentTrigger);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                        break;
                }

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    if (persistentTrigger.State == PersistentTriggerState.Blocked)
                    {
                        persistentTrigger.State = PersistentTriggerState.Waiting;
                        await _triggerRepository.Update(persistentTrigger);
                    }
                    
                    if (persistentTrigger.State == PersistentTriggerState.PausedBlocked)
                    {
                        persistentTrigger.State = PersistentTriggerState.Paused;
                        await _triggerRepository.Update(persistentTrigger);
                    }
                    
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                }

                if (jobDetail.PersistJobDataAfterExecution && jobDetail.JobDataMap.Dirty)
                {
                    var persistentJobDetail = await _jobRepository.Get(PersistentJob.GetId(InstanceName, jobDetail.Key));
                    persistentJobDetail.JobDataMap = jobDetail.JobDataMap;
                    await _jobRepository.Update(persistentJobDetail);
                }
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }

            try
            {
                await _firedTriggerRepository.Delete(PersistentFiredTrigger.GetId(InstanceName, trigger.FireInstanceId));
            }
            catch (Exception ex)
            {
                throw new JobPersistenceException(ex.Message, ex);
            }
        }
        
        protected virtual void SignalSchedulingChangeOnTxCompletion(DateTimeOffset? candidateNewNextFireTime)
        {
            var sigTime = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
            if (sigTime == null && candidateNewNextFireTime.HasValue)
            {
                LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
            }
            else
            {
                if (sigTime == null || candidateNewNextFireTime < sigTime)
                {
                    LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
                }
            }
        }
        
        private async Task<TriggerFiredBundle> TriggerFiredInternal(IOperableTrigger trigger)
        {
            var persistentTrigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, trigger.Key));
            if (persistentTrigger == null || persistentTrigger.State != PersistentTriggerState.Acquired)
            {
                return null;
            }

            var job = await _jobRepository.Get(PersistentJob.GetId(InstanceName, trigger.JobKey));
            if (job == null)
            {
                return null;
            }

            ICalendar calendar = null;
            if (trigger.CalendarName != null)
            {
                calendar = (await _calendarRepository.Get(PersistentCalendar.GetId(InstanceName, trigger.CalendarName)))?.Calendar;
                if (calendar == null)
                {
                    return null;
                }
            }

            await _firedTriggerRepository.Update(
                new PersistentFiredTrigger(trigger.FireInstanceId,
                    TriggerFactory.CreateTrigger(trigger, PersistentTriggerState.Executing, InstanceName), job)
                {
                    InstanceId = InstanceId,
                    State = PersistentTriggerState.Executing
                });

            var prevFireTime = trigger.GetPreviousFireTimeUtc();
            trigger.Triggered(calendar);

            var state = PersistentTriggerState.Waiting;
            var force = true;

            if (job.ConcurrentExecutionDisallowed)
            {
                state = PersistentTriggerState.Blocked;
                force = false;
                
                if (persistentTrigger.State == PersistentTriggerState.Waiting ||
                    persistentTrigger.State == PersistentTriggerState.Acquired)
                {
                    persistentTrigger.State = PersistentTriggerState.Blocked;
                    await _triggerRepository.Update(persistentTrigger);
                } 
                else if (persistentTrigger.State == PersistentTriggerState.Paused)
                {
                    persistentTrigger.State = PersistentTriggerState.PausedBlocked;
                    await _triggerRepository.Update(persistentTrigger);
                }
            }

            if (!trigger.GetNextFireTimeUtc().HasValue)
            {
                state = PersistentTriggerState.Complete;
                force = true;
            }

            var jobDetail = job.GetJobDetail();
            await StoreTriggerInternal(trigger, jobDetail, true, state, force, force);

            jobDetail.JobDataMap.ClearDirtyFlag();

            return new TriggerFiredBundle(jobDetail,
                trigger,
                calendar,
                trigger.Key.Group.Equals(SchedulerConstants.DefaultRecoveryGroup),
                DateTimeOffset.UtcNow,
                trigger.GetPreviousFireTimeUtc(),
                prevFireTime,
                trigger.GetNextFireTimeUtc());
        }
        
        private async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersInternal(
            DateTimeOffset noLaterThan, int maxCount,
            TimeSpan timeWindow)
        {
            if (timeWindow < TimeSpan.Zero)
            {
                throw new ArgumentOutOfRangeException(nameof(timeWindow));
            }

            var acquiredTriggers = new List<IOperableTrigger>();
            var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

            const int maxDoLoopRetry = 3;
            var currentLoopCount = 0;

            do
            {
                currentLoopCount++;
                var triggers = await _triggerRepository.GetAllToAcquire(noLaterThan + timeWindow, MisfireTime, maxCount);

                if (!triggers.Any())
                {
                    return acquiredTriggers;
                }

                foreach (var nextTrigger in triggers)
                {
                    var jobKey = nextTrigger.GetJobKey();
                    PersistentJob job;
                    try
                    {
                        job = await _jobRepository.Get(PersistentJob.GetId(InstanceName, nextTrigger.GetJobKey()));
                    }
                    catch (Exception)
                    {
                        nextTrigger.State = PersistentTriggerState.Error;
                        await _triggerRepository.Update(nextTrigger);
                        continue;
                    }

                    if (job.ConcurrentExecutionDisallowed)
                    {
                        if (acquiredJobKeysForNoConcurrentExec.Contains(jobKey))
                        {
                            continue;
                        }

                        acquiredJobKeysForNoConcurrentExec.Add(jobKey);
                    }

                    if (nextTrigger.State == PersistentTriggerState.Waiting)
                    {
                        nextTrigger.State = PersistentTriggerState.Acquired;
                        await _triggerRepository.Update(nextTrigger);
                    }
                    else
                    {
                        continue;
                    }

                    var operableTrigger = (IOperableTrigger) nextTrigger.GetTrigger();
                    operableTrigger.FireInstanceId = GetFiredTriggerRecordId();

                    var firedTrigger = new PersistentFiredTrigger(operableTrigger.FireInstanceId, nextTrigger, null)
                    {
                        State = PersistentTriggerState.Acquired,
                        InstanceId = InstanceId
                    };
                    await _firedTriggerRepository.Save(firedTrigger);

                    acquiredTriggers.Add(operableTrigger);
                }

                if (acquiredTriggers.Count == 0 && currentLoopCount < maxDoLoopRetry)
                {
                    continue;
                }

                break;
            } while (true);

            return acquiredTriggers;
        }

        private string GetFiredTriggerRecordId()
        {
            Interlocked.Increment(ref _fireTriggerRecordCounter);
            return InstanceId + _fireTriggerRecordCounter;
        }
        
        private async Task<IReadOnlyCollection<string>> ResumeTriggersInternal(GroupMatcher<TriggerKey> matcher)
        {
            var pausedTriggerGroups = (await _pausedTriggerGroupRepository.GetGroups()).Where(x => matcher.IsMatch(new TriggerKey("not_used", x))); // This is not very optimal
            foreach (var group in pausedTriggerGroups)
            {
                await _pausedTriggerGroupRepository.Delete(PausedTriggerGroup.GetId(InstanceName, group));
            }
            
            var groups = new HashSet<string>();

            var triggers = (await _triggerRepository.GetAll()).Where(x => matcher.IsMatch(x.GetTriggerKey())); // This is again not very optimal, but we optimize using IN (groups)
            foreach (var triggerKey in triggers)
            {
                await ResumeTriggerInternal(triggerKey);
                groups.Add(triggerKey.Group);
            }

            return groups.ToList();
        }
        
        private async Task ResumeTriggerInternal(PersistentTriggerBase trigger)
        {
            if (trigger?.NextFireTime == null || trigger.NextFireTime == DateTimeOffset.MinValue)
            {
                return;
            }

            var blocked = trigger.State == PersistentTriggerState.PausedBlocked;
            var newState = await CheckBlockedState(trigger.GetJobKey(), PersistentTriggerState.Waiting);
            var misfired = false;

            if (_schedulerRunning && trigger.NextFireTime < DateTimeOffset.UtcNow)
            {
                misfired = await UpdateMisfiredTrigger(trigger, newState, true);
            }

            if (!misfired)
            {
                if (trigger.State == (blocked ? PersistentTriggerState.PausedBlocked : PersistentTriggerState.Paused))
                {
                    trigger.State = newState;
                    await _triggerRepository.Update(trigger);                    
                }
            }
        }
        
        private async Task<bool> UpdateMisfiredTrigger(PersistentTriggerBase trigger, PersistentTriggerState newStateIfNotComplete,
            bool forceState)
        {
            var misfireTime = DateTimeOffset.UtcNow;
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            if (trigger.NextFireTime > misfireTime)
            {
                return false;
            }

            await DoUpdateOfMisfiredTrigger(trigger, forceState, newStateIfNotComplete, false);

            return true;
        }
        
        private async Task RecoverJobs()
        {
            using (await _lockManager.AcquireLock(LockType.TriggerAccess))
            {
                var result = await _triggerRepository.UpdateAllByStates(PersistentTriggerState.Waiting, PersistentTriggerState.Acquired, PersistentTriggerState.Blocked);
                
                result += await _triggerRepository.UpdateAllByStates(PersistentTriggerState.Paused, PersistentTriggerState.PausedBlocked);

                _logger.Info($"Freed {result} triggers from 'acquired' / 'blocked' state.");

                await RecoverMisfiredJobsInternal(true);

                var results = (await _firedTriggerRepository.GetAllRecoverableByInstanceId(InstanceId))
                    .Select(async trigger => trigger.GetRecoveryTrigger((await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, trigger.GetTriggerKey()))).JobDataMap));
                var recoveringJobTriggers = (await Task.WhenAll(results)).ToList();

                _logger.Info($"Recovering {recoveringJobTriggers.Count} jobs that were in-progress at the time of the last shut-down.");

                foreach (var recoveringJobTrigger in recoveringJobTriggers)
                    if (await _jobRepository.Exists(PersistentJob.GetId(InstanceName, recoveringJobTrigger.JobKey)))
                    {
                        recoveringJobTrigger.ComputeFirstFireTimeUtc(null);
                        await StoreTriggerInternal(recoveringJobTrigger, null, false, PersistentTriggerState.Waiting, false, true);
                    }

                _logger.Info("Recovery complete");

                var completedTriggers = await _triggerRepository.GetAllByState(PersistentTriggerState.Complete);
                foreach (var completedTrigger in completedTriggers)
                {
                    await RemoveTriggerInternal(completedTrigger);
                }

                _logger.Info($"Removed {completedTriggers.Count} 'complete' triggers.");

                result = await _firedTriggerRepository.DeleteAllByInstanceId(InstanceId);
                _logger.Info($"Removed {result} stale fired job entries.");
            }
        }
        
        private async Task<bool> RemoveTriggerInternal(PersistentTriggerBase trigger, IJobDetail job = null)
        {
            if (job == null)
            {
                var result = await _jobRepository.Get(PersistentJob.GetId(InstanceName, trigger.GetJobKey()));
                job = result?.GetJobDetail();
            }

            var deleted = await _triggerRepository.Delete(trigger.Id);

            if (job != null && !job.Durable)
            {
                if (await _triggerRepository.CountByJob(job.Key.Name, job.Key.Group) == 0)
                {
                    if (await _jobRepository.Delete(PersistentJob.GetId(InstanceName, job.Key)))
                    {
                        await _schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key);
                    }
                }
            }

            return deleted;
        }
        
        private async Task<bool> RemoveJobWithTriggersInternal(string jobName, string jobGroup)
        {
            var triggers = await _triggerRepository.GetAllByJob(jobName, jobGroup);

            foreach (var trigger in triggers)
            {
                await _triggerRepository.Delete(trigger.Id);                
            }
            
            return await _jobRepository.Delete(PersistentJob.GetId(InstanceName, jobGroup, jobName));
        }
        
        private async Task<RecoverMisfiredJobsResult> RecoverMisfiredJobsInternal(bool recovering)
        {
            var maxMisfiresToHandleAtTime = recovering ? 0 : MaxMisfiresToHandleAtATime; // Changed according to former implementation from -1
            var earliestNewTime = DateTimeOffset.MaxValue;

            var misfiredTriggers = await _triggerRepository.GetAllMisfired(MisfireTime, maxMisfiresToHandleAtTime + 1); // Get one more to know if there are any / more
            var hasMoreMisfiredTriggers = misfiredTriggers.Count > maxMisfiresToHandleAtTime;
            misfiredTriggers = misfiredTriggers.Take(maxMisfiresToHandleAtTime).ToList(); // Correct to appropriate length

            if (hasMoreMisfiredTriggers)
            {
                _logger.Info($"Handling the first {misfiredTriggers.Count} triggers that missed their scheduled fire-time. More misfired triggers remain to be processed.");
            }
            else if (misfiredTriggers.Count > 0)
            {
                _logger.Info($"Handling {misfiredTriggers.Count} trigger(s) that missed their scheduled fire-time.");
            }
            else
            {
                _logger.Debug("Found 0 triggers that missed their scheduled fire-time.");
                return RecoverMisfiredJobsResult.NoOp;
            }

            foreach (var trigger in misfiredTriggers)
            {
                await DoUpdateOfMisfiredTrigger(trigger, false, PersistentTriggerState.Waiting, recovering);

                var nextTime = trigger.NextFireTime;
                if (nextTime.HasValue && nextTime.Value < earliestNewTime)
                {
                    earliestNewTime = nextTime.Value;
                }
            }

            return new RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.Count, earliestNewTime);
        }
        
        private async Task DoUpdateOfMisfiredTrigger(PersistentTriggerBase trigger, bool forceState, PersistentTriggerState newStateIfNotComplete, bool recovering)
        {
            var operableTrigger = (IOperableTrigger) trigger.GetTrigger();

            ICalendar cal = null;
            if (trigger.CalendarName != null)
            {
                cal = (await _calendarRepository.Get(PersistentCalendar.GetId(InstanceName, trigger.CalendarName)))?.Calendar;
            }

            await _schedulerSignaler.NotifyTriggerListenersMisfired(operableTrigger);
            operableTrigger.UpdateAfterMisfire(cal);

            if (!operableTrigger.GetNextFireTimeUtc().HasValue)
            {
                await StoreTriggerInternal(operableTrigger, null, true, PersistentTriggerState.Complete, forceState, recovering);
                await _schedulerSignaler.NotifySchedulerListenersFinalized(operableTrigger);
            }
            else
            {
                await StoreTriggerInternal(operableTrigger, null, true, newStateIfNotComplete, forceState, false);
            }
        }
        
        private async Task<PersistentTriggerState> CheckBlockedState(JobKey jobKey, PersistentTriggerState currentState)
        {
            if (currentState != PersistentTriggerState.Waiting && currentState != PersistentTriggerState.Paused)
            {
                return currentState;
            }

            var firedTrigger = (await _firedTriggerRepository.GetAllByJob(jobKey.Name, jobKey.Group)).FirstOrDefault();
            
            if (firedTrigger != null)
            {
                if (firedTrigger.ConcurrentExecutionDisallowed)
                {
                    return currentState == PersistentTriggerState.Paused
                        ? PersistentTriggerState.PausedBlocked
                        : PersistentTriggerState.Blocked;
                }
            }

            return currentState;
        }
        
        private async Task PauseTriggerInternal(string name, string group)
        {
            var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, group, name));
            switch (trigger.State)
            {
                case PersistentTriggerState.Waiting:
                case PersistentTriggerState.Acquired:
                    trigger.State = PersistentTriggerState.Paused;
                    await _triggerRepository.Update(trigger);
                    break;
                case PersistentTriggerState.Blocked:
                    trigger.State = PersistentTriggerState.PausedBlocked;
                    await _triggerRepository.Update(trigger);
                    break;
            }
        }
        
        //---------------------------------------------------------------------------
        // Cluster management methods (from JobStoreSupport)
        //---------------------------------------------------------------------------

        protected bool firstCheckIn = true;

        protected internal DateTimeOffset LastCheckin { get; set; } = SystemTime.UtcNow();

        protected internal virtual async Task<bool> DoCheckin(Guid requestorId, CancellationToken cancellationToken = default)
        {
            // bool transOwner = false;
            // bool transStateOwner = false;
            bool recovered = false;

            
            // Other than the first time, always checkin first to make sure there is
            // work to be done before we acquire the lock (since that is expensive,
            // and is almost never necessary). This must be done in a separate
            // transaction to prevent a deadlock under recovery conditions.
            IReadOnlyList<PersistentScheduler> failedSchedulers = null;
            if (!firstCheckIn)
            {
                failedSchedulers = await ClusterCheckIn(cancellationToken).ConfigureAwait(false);
            }

            if (firstCheckIn || failedSchedulers != null && failedSchedulers.Count > 0)
            {
                using (await _lockManager.AcquireLock(LockType.StateAccess))
                {
                    // transStateOwner = true;

                    // Now that we own the lock, make sure we still have work to do.
                    // The first time through, we also need to make sure we update/create our state record
                    if (firstCheckIn)
                    {
                        failedSchedulers = await ClusterCheckIn(cancellationToken).ConfigureAwait(false);
                    }
                    else
                    {
                        failedSchedulers = await FindFailedInstances(cancellationToken).ConfigureAwait(false);
                    }

                    if (failedSchedulers.Count > 0)
                    {
                        using (await _lockManager.AcquireLock(LockType.TriggerAccess))
                        {
                            // getLockHandler().obtainLock(conn, LockJobAccess);
                            // transOwner = true;

                            await ClusterRecover(failedSchedulers, cancellationToken).ConfigureAwait(false);
                            recovered = true;                        
                        }
                    }
                }
            }

            firstCheckIn = false;

            return recovered;
        }

        /// <summary>
        /// Get a list of all scheduler instances in the cluster that may have failed.
        /// This includes this scheduler if it is checking in for the first time.
        /// </summary>
        protected virtual async Task<IReadOnlyList<PersistentScheduler>> FindFailedInstances(CancellationToken cancellationToken = default)
        {
            try
            {
                var failedInstances = new List<PersistentScheduler>();
                var foundThisScheduler = false;

                var schedulers = await _schedulerRepository.GetAll();

                foreach (var scheduler in schedulers)
                {
                    // find own record...
                    if (scheduler.InstanceId == InstanceId)
                    {
                        foundThisScheduler = true;
                        if (firstCheckIn)
                        {
                            failedInstances.Add(scheduler);
                        }
                    }
                    else
                    {
                        // find failed instances...
                        if (CalcFailedIfAfter(scheduler) < DateTimeOffset.UtcNow)
                        {
                            failedInstances.Add(scheduler);
                        }
                    }
                }

                // The first time through, also check for orphaned fired triggers.
                if (firstCheckIn)
                {
                    var orphanedInstances = await FindOrphanedFailedInstances(schedulers).ConfigureAwait(false);
                    failedInstances.AddRange(orphanedInstances);
                }

                // If not the first time but we didn't find our own instance, then
                // Someone must have done recovery for us.
                if (!foundThisScheduler && !firstCheckIn)
                {
                    // TODO: revisit when handle self-failed-out impl'ed (see TODO in clusterCheckIn() below)
                    _logger.Warn(
                        $"This scheduler instance ({InstanceId}) is still active but was recovered by another instance in the cluster.  This may cause inconsistent behavior.");
                }

                return failedInstances;
            }
            catch (Exception e)
            {
                LastCheckin = DateTimeOffset.UtcNow;
                throw new JobPersistenceException($"Failure identifying failed instances when checking-in: {e.Message}", e);
            }
        }

        /// <summary>
        /// Create dummy <see cref="SchedulerStateRecord" /> objects for fired triggers
        /// that have no scheduler state record.  Checkin timestamp and interval are
        /// left as zero on these dummy <see cref="SchedulerStateRecord" /> objects.
        /// </summary>
        /// <param name="schedulers">List of all current <see cref="SchedulerStateRecord" />s</param>
        private async Task<IReadOnlyList<PersistentScheduler>> FindOrphanedFailedInstances(
            IEnumerable<PersistentScheduler> schedulers)
        {
            var orphanedInstances = new List<PersistentScheduler>();

            var allFiredTriggerInstanceNames = (await _firedTriggerRepository.GetAll()).Select(x => x.InstanceId).Distinct().ToList();
            if (allFiredTriggerInstanceNames.Count > 0)
            {
                foreach (var scheduler in schedulers)
                {
                    allFiredTriggerInstanceNames.Remove(scheduler.InstanceId);
                }

                foreach (var name in allFiredTriggerInstanceNames)
                {
                    var orphanedInstance = new PersistentScheduler(InstanceName, name);
                    orphanedInstances.Add(orphanedInstance);

                    _logger.Warn($"Found orphaned fired triggers for instance: {orphanedInstance.InstanceId}");
                }
            }

            return orphanedInstances;
        }

        private DateTimeOffset CalcFailedIfAfter(PersistentScheduler scheduler)
        {
            var passed = DateTimeOffset.UtcNow - LastCheckin;
            var ts = scheduler.CheckinInterval > passed ? scheduler.CheckinInterval : passed; // Max
            return scheduler.LastCheckIn.Add(ts).Add(ClusterCheckinMisfireThreshold);
        }

        protected virtual async Task<IReadOnlyList<PersistentScheduler>> ClusterCheckIn(CancellationToken cancellationToken = default)
        {
            var failedInstances = await FindFailedInstances(cancellationToken).ConfigureAwait(false);
            try
            {
                // TODO: handle self-failed-out

                // check in...
                LastCheckin = DateTimeOffset.UtcNow;
                var persistentScheduler = await _schedulerRepository.Get(PersistentScheduler.GetId(InstanceName, InstanceId));
                if (persistentScheduler == null)
                {
                    persistentScheduler = new PersistentScheduler(InstanceName, InstanceId)
                    {
                        LastCheckIn = LastCheckin, 
                        CheckinInterval = ClusterCheckinInterval
                    };
                    await _schedulerRepository.Save(persistentScheduler);
                }
                else
                {
                    persistentScheduler.CheckinInterval = ClusterCheckinInterval;
                    persistentScheduler.LastCheckIn = LastCheckin;
                    await _schedulerRepository.Update(persistentScheduler);
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException("Failure updating scheduler state when checking-in: " + e.Message, e);
            }

            return failedInstances;
        }

        protected virtual async Task ClusterRecover(IReadOnlyList<PersistentScheduler> failedInstances, CancellationToken cancellationToken = default)
        {
            if (failedInstances.Count <= 0)
            {
                return;
            }

            long recoverIds = DateTimeOffset.UtcNow.Ticks;

            LogWarnIfNonZero(failedInstances.Count, $"ClusterManager: detected {failedInstances.Count} failed or restarted instances.");
            try
            {
                foreach (var scheduler in failedInstances)
                {
                    _logger.Info($"ClusterManager: Scanning for instance \"{scheduler.InstanceId}\"\'s failed in-progress jobs.");

                    var firedTriggers = await _firedTriggerRepository.GetAllByInstanceId(scheduler.InstanceId);

                    int acquiredCount = 0;
                    int recoveredCount = 0;
                    int otherCount = 0;

                    var triggerKeys = new HashSet<TriggerKey>();

                    foreach (var firedTrigger in firedTriggers)
                    {
                        TriggerKey tKey = firedTrigger.GetTriggerKey();
                        JobKey jKey = firedTrigger.GetJobKey();

                        triggerKeys.Add(tKey);

                        // release blocked triggers..
                        if (firedTrigger.State == PersistentTriggerState.Blocked)
                        {
                            await UpdateTriggerStatesForJobFromOtherState(jKey, PersistentTriggerState.Waiting, PersistentTriggerState.Blocked);
                        }
                        else if (firedTrigger.State == PersistentTriggerState.PausedBlocked)
                        {
                            await UpdateTriggerStatesForJobFromOtherState(jKey, PersistentTriggerState.Paused, PersistentTriggerState.PausedBlocked);
                        }

                        // release acquired triggers..
                        if (firedTrigger.State == PersistentTriggerState.Acquired)
                        {
                            await UpdateTriggerStatesForJobFromOtherState(jKey, PersistentTriggerState.Waiting, PersistentTriggerState.Acquired);
                            acquiredCount++;
                        }
                        else if (firedTrigger.RequestsRecovery)
                        {
                            // handle jobs marked for recovery that were not fully executed..
                            if (await _jobRepository.Exists(PersistentJob.GetId(InstanceName, jKey)))
                            {
                                var rcvryTrig =
                                    new SimpleTriggerImpl(
                                        $"recover_{scheduler.InstanceId}_{Convert.ToString(recoverIds++, CultureInfo.InvariantCulture)}",
                                        SchedulerConstants.DefaultRecoveryGroup, firedTrigger.Fired)
                                    {
                                        JobName = jKey.Name,
                                        JobGroup = jKey.Group,
                                        MisfireInstruction = MisfireInstruction.SimpleTrigger.FireNow,
                                        Priority = firedTrigger.Priority
                                    };

                                var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, tKey));

                                var jd = trigger.JobDataMap;
                                jd.Put(SchedulerConstants.FailedJobOriginalTriggerName, tKey.Name);
                                jd.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, tKey.Group);
                                jd.Put(SchedulerConstants.FailedJobOriginalTriggerFiretime,
                                    Convert.ToString(firedTrigger.Fired, CultureInfo.InvariantCulture));
                                rcvryTrig.JobDataMap = jd;

                                rcvryTrig.ComputeFirstFireTimeUtc(null);
                                await StoreTriggerInternal(rcvryTrig, null, false, PersistentTriggerState.Waiting, false, true, cancellationToken);
                                recoveredCount++;
                            }
                            else
                            {
                                _logger.Warn($"ClusterManager: failed job \'{jKey}\' no longer exists, cannot schedule recovery.");
                                otherCount++;
                            }
                        }
                        else
                        {
                            otherCount++;
                        }

                        // free up stateful job's triggers
                        if (firedTrigger.ConcurrentExecutionDisallowed)
                        {
                            await UpdateTriggerStatesForJobFromOtherState(jKey, PersistentTriggerState.Waiting, PersistentTriggerState.Blocked);
                            await UpdateTriggerStatesForJobFromOtherState(jKey, PersistentTriggerState.Paused, PersistentTriggerState.PausedBlocked);
                        }

                        await _firedTriggerRepository.Delete(firedTrigger.Id);
                    }

                    // Check if any of the fired triggers we just deleted were the last fired trigger
                    // records of a COMPLETE trigger.
                    int completeCount = 0;
                    foreach (var triggerKey in triggerKeys)
                    {
                        var trigger = await _triggerRepository.Get(PersistentTriggerBase.GetId(InstanceName, triggerKey));
                        
                        if (trigger.State == PersistentTriggerState.Complete)
                        {
                            var frdTrgrs = await _firedTriggerRepository.GetAllByTrigger(triggerKey.Name, triggerKey.Group);
                            if (frdTrgrs.Count == 0)
                            {
                                if (await RemoveTriggerInternal(trigger))
                                {
                                    completeCount++;
                                }
                            }
                        }
                    }

                    LogWarnIfNonZero(acquiredCount, $"ClusterManager: ......Freed {acquiredCount} acquired trigger(s).");
                    LogWarnIfNonZero(completeCount, $"ClusterManager: ......Deleted {completeCount} complete triggers(s).");
                    LogWarnIfNonZero(recoveredCount, $"ClusterManager: ......Scheduled {recoveredCount} recoverable job(s) for recovery.");
                    LogWarnIfNonZero(otherCount, $"ClusterManager: ......Cleaned-up {otherCount} other failed job(s).");

                    if (scheduler.InstanceId != InstanceId)
                    {
                        await _schedulerRepository.Delete(scheduler.Id);
                    }
                }
            }
            catch (Exception e)
            {
                throw new JobPersistenceException($"Failure recovering jobs: {e.Message}", e);
            }
        }

        private async Task UpdateTriggerStatesForJobFromOtherState(JobKey jKey, PersistentTriggerState to, PersistentTriggerState from)
        {
            var triggers = await _triggerRepository.GetAllByJobAndState(jKey, from);
            foreach (var trigger in triggers)
            {
                trigger.State = to;
                await _triggerRepository.Update(trigger);
            }
        }

        private static void LogWarnIfNonZero(int val, string warning)
        {
            if (val > 0)
            {
                _logger.Info(warning);
            }
            else
            {
                _logger.Debug(warning);
            }
        }
    }
}
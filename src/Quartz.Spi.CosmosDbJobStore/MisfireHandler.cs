using System;
using System.Threading;
using Common.Logging;
using Quartz.Impl.AdoJobStore;
using Quartz.Spi.CosmosDbJobStore.Util;

namespace Quartz.Spi.CosmosDbJobStore
{
    /// <summary>
    /// 
    /// </summary>
    internal class MisfireHandler : QuartzThread
    {
        private static readonly ILog _logger = LogManager.GetLogger<MisfireHandler>();
        
        private readonly CosmosDbJobStore _jobStore;
        private bool _shutdown;
        private int _numFails;

        
        public MisfireHandler(CosmosDbJobStore jobStore)
        {
            _jobStore = jobStore;
            Name = $"QuartzScheduler_{jobStore.InstanceName}-{jobStore.InstanceId}_MisfireHandler";
            IsBackground = true;
        }

        
        public void Shutdown()
        {
            _shutdown = true;
            Interrupt();
        }

        public override void Run()
        {
            while (!_shutdown)
            {
                var now = DateTimeOffset.UtcNow;
                var recoverResult = Manage();
                if (recoverResult.ProcessedMisfiredTriggerCount > 0)
                {
                    _jobStore.SignalSchedulingChangeImmediately(recoverResult.EarliestNewTime);
                }

                if (!_shutdown)
                {
                    var timeToSleep = TimeSpan.FromMilliseconds(50);
                    if (!recoverResult.HasMoreMisfiredTriggers)
                    {
                        timeToSleep = _jobStore.MisfireThreshold - (DateTime.UtcNow - now);
                        if (timeToSleep <= TimeSpan.Zero)
                        {
                            timeToSleep = TimeSpan.FromMilliseconds(50);
                        }

                        if (_numFails > 0)
                        {
                            timeToSleep = _jobStore.DbRetryInterval > timeToSleep
                                ? _jobStore.DbRetryInterval
                                : timeToSleep;
                        }
                    }

                    try
                    {
                        Thread.Sleep(timeToSleep);
                    }
                    catch (ThreadInterruptedException)
                    {
                    }
                }
            }
        }

        private RecoverMisfiredJobsResult Manage()
        {
            try
            {
                _logger.Debug("Scanning for misfires...");
                var result = _jobStore.DoRecoverMisfires().Result;
                _numFails = 0;
                return result;
            }
            catch (Exception ex)
            {
                if (_numFails % _jobStore.RetryableActionErrorLogThreshold == 0)
                {
                    _logger.Error($"Error handling misfires: {ex.Message}", ex);
                }
                _numFails++;
            }

            return RecoverMisfiredJobsResult.NoOp;
        }
    }
}
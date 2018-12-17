using System;
using System.Threading.Tasks;
using Common.Logging;

namespace Quartz.Spi.CosmosDbJobStore.Tests.Jobs
{
    [DisallowConcurrentExecution]
    public class UnreliableJob : IJob
    {
        private static readonly ILog _logger = LogManager.GetLogger<UnreliableJob>();
        
        public static int TimesRun;

        public static bool Finished;
        
        
        public async Task Execute(IJobExecutionContext context)
        {
            if (++TimesRun <= 1)
            {               
                var t = context.Trigger.GetTriggerBuilder()
                    .StartAt(DateTimeOffset.Now.AddSeconds(5))
                    .Build();

                await context.Scheduler.RescheduleJob(t.Key, t);
                
                _logger.Info("I have died, but I will be resurrected in 5 seconds :-P");
                
                // throw new JobExecutionException(false); According to Quartz.NET best practices, we should handle retry ourselves 
            }
            else
            {
                Finished = true;                
            }
            
        }
    }
}
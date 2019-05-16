using System.Threading.Tasks;

namespace Quartz.Spi.CosmosDbJobStore.Tests.Jobs
{
    [DisallowConcurrentExecution]
    public class HardJob : IJob
    {
        internal static int TimesExecuted = 0;
        
        public async Task Execute(IJobExecutionContext context)
        {
            ++TimesExecuted;
            
            await Task.Delay(12 * 60 * 1000); // By this 12 minutes we simulate real issue, when the former job was not completed but another execution has started about 9 minutes from first start 
        }
    }
}
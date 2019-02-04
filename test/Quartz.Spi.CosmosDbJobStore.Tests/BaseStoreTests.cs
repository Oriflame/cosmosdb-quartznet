using System;
using System.Collections.Specialized;
using System.Threading.Tasks;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected static Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var properties = new NameValueCollection
            {
                [$"{HackedStdSchedulerFactory.PropertyObjectSerializer}.type"] = "json",
                [HackedStdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
                [HackedStdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
                [HackedStdSchedulerFactory.PropertyJobStoreType] = typeof(CosmosDbJobStore).AssemblyQualifiedName,
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Endpoint"] = "https://localhost:8081/",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Key"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.DatabaseId"] = "quartz-demo",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.CollectionId"] = "Quartz",
                [$"{HackedStdSchedulerFactory.PropertyJobStorePrefix}.Clustered"] = "true"
            };

            var scheduler = new HackedStdSchedulerFactory(properties);
            return scheduler.GetScheduler();
        }
    }
}
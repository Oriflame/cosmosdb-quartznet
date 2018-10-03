using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Quartz.Impl;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public abstract class BaseStoreTests
    {
        public const string Barrier = "BARRIER";
        public const string DateStamps = "DATE_STAMPS";
        public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

        protected async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
        {
            var properties = new NameValueCollection
            {
                ["quartz.serializer.type"] = "json",
                [StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
                [StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
                [StdSchedulerFactory.PropertyJobStoreType] = typeof(CosmosDbJobStore).AssemblyQualifiedName,
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.Endpoint"] = "https://localhost:8081/",
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.Key"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.DatabaseId"] = "quartz-demo",
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.CollectionId"] = "Quartz",
                [$"{StdSchedulerFactory.PropertyJobStorePrefix}.Clustered"] = "true"
            };

            var scheduler = new StdSchedulerFactory(properties);
            return await scheduler.GetScheduler();
        }
    }
}
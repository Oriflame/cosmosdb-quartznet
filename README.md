CosmosDb Job Store for Quartz.NET
================================

We have created this project to allow clustered Quartz.NET store jobs into CosmosDb. It is more or less port of [Quartz.NET MongoDb Job Store](https://github.com/chrisdrobison/mongodb-quartz-net).   

## Basic Usage##

```cs
var properties = new NameValueCollection();
properties[StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName;
properties[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
properties[StdSchedulerFactory.PropertyJobStoreType] = typeof(CosmosDbJobStore).AssemblyQualifiedName,
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Endpoint"] = "https://localhost:8081/",
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Key"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==",
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.DatabaseId"] = "quartz-demo",
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.CollectionId"] = "Quartz",
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Clustered"] = "true"

var scheduler = new StdSchedulerFactory(properties);
return scheduler.GetScheduler();
```

## Nuget ##

```
Install-Package Quartz.Spi.CosmosDbJobStore
```
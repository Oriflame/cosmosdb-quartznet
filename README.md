CosmosDb Job Store for Quartz.NET
[![Downloads](https://img.shields.io/nuget/dt/Quartz.Spi.CosmosDbJobStore.svg)](https://www.nuget.org/packages/Quartz.Spi.CosmosDbJobStore/)
================================

We have created this project to allow clustered Quartz.NET store jobs into CosmosDb. It is more or less port of [Quartz.NET MongoDb Job Store](https://github.com/chrisdrobison/mongodb-quartz-net).   

## Basic Usage

```cs
var properties = new NameValueCollection();
properties[StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName;
properties[StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}";
properties[StdSchedulerFactory.PropertyJobStoreType] = typeof(CosmosDbJobStore).AssemblyQualifiedName;
properties[$"{StdSchedulerFactory.PropertyObjectSerializer}.type"] = "json";
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Endpoint"] = "https://localhost:8081/";
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Key"] = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.DatabaseId"] = "quartz-demo";
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.CollectionId"] = "Quartz";
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.Clustered"] = "true";

var scheduler = new StdSchedulerFactory(properties);
return scheduler.GetScheduler();
```
### Collection partition key
By default Cosmos DB collection partitioned by `PropertySchedulerInstanceName`, instead entity type (e.g. 'Trigger', 'JobDetails') can be used as the partition key.
```cs
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.{nameof(CosmosDbJobStore.PartitionPerEntityType)}"] = "true";
```
### ConnectionMode
By default the Cosmos DB connectionMode is `Direct`, instead `Gateway` can be used as connectionMode.
```cs
properties[$"{StdSchedulerFactory.PropertyJobStorePrefix}.ConnectionMode"] = ((int)ConnectionMode.Gateway).ToString();
```

## Nuget ##

```
Install-Package Quartz.Spi.CosmosDbJobStore
```

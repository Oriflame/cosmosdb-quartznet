using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Common.Logging;
using FluentAssertions;
using Quartz.Impl.Matchers;
using Quartz.Logging;
using Quartz.Spi.CosmosDbJobStore.Tests.Jobs;
using Xunit;
using Xunit.Abstractions;

[assembly: CollectionBehavior(DisableTestParallelization = true)]

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public class CosmosDbJobStoreTests : BaseStoreTests, IDisposable
    {
        private readonly Random _random = new Random();
        private readonly IScheduler[] _schedulers;
        
        
        /// <summary>
        /// Get random scheduler.
        /// </summary>
        private IScheduler Scheduler => _schedulers[_random.Next(_schedulers.Length)];


        public CosmosDbJobStoreTests(ITestOutputHelper output)
        {
            LogProvider.SetCurrentLogProvider(new XunitConsoleLogProvider(output)); // Setup Quartz.NET logger
            LogManager.Adapter = new XunitConsoleLogAdapter(output); // Setup Common.logging

            _schedulers = new IScheduler[5];
            
            for (var i = 0; i < _schedulers.Length; i++)
            {
                _schedulers[i] = CreateScheduler().Result;
                _schedulers[i].Clear().Wait();
            }
        }

        public void Dispose()
        {
            foreach (var scheduler in _schedulers)
            {
                scheduler.Shutdown().Wait();
            }
        }

        [Fact]
        public async Task AddJobTest()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            (await Scheduler.CheckExists(new JobKey("j1"))).Should().BeFalse();

            await Scheduler.AddJob(job, false);

            (await Scheduler.CheckExists(new JobKey("j1"))).Should().BeTrue();
        }

        [Fact]
        public async Task RetrieveJobTest()
        {
           var job = JobBuilder.Create<SimpleJob>()
               .WithIdentity("j1")
               .StoreDurably()
               .Build();
           await Scheduler.AddJob(job, false);

           job = await Scheduler.GetJobDetail(new JobKey("j1"));

           job.Should().NotBeNull();
        }

        [Fact]
        public async Task AddTriggerTest()
        {
           var job = JobBuilder.Create<SimpleJob>()
               .WithIdentity("j1")
               .StoreDurably()
               .Build();

           var trigger = TriggerBuilder.Create()
               .WithIdentity("t1")
               .ForJob(job)
               .StartNow()
               .WithSimpleSchedule(x => x
                   .RepeatForever()
                   .WithIntervalInSeconds(5))
               .Build();

           (await Scheduler.CheckExists(new TriggerKey("t1"))).Should().BeFalse();

           await Scheduler.ScheduleJob(job, trigger);

           (await Scheduler.CheckExists(new TriggerKey("t1"))).Should().BeTrue();

           job = await Scheduler.GetJobDetail(new JobKey("j1"));

           job.Should().NotBeNull();

           trigger = await Scheduler.GetTrigger(new TriggerKey("t1"));

           trigger.Should().NotBeNull();
        }

        [Fact]
        public async Task GroupsTest()
        {
           await CreateJobsAndTriggers();

           var jobGroups = await Scheduler.GetJobGroupNames();
           var triggerGroups = await Scheduler.GetTriggerGroupNames();

           jobGroups.Count.Should().Be(2, "Job group list size expected to be = 2 ");
           triggerGroups.Count.Should().Be(2, "Trigger group list size expected to be = 2 ");

           var jobKeys = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
           var triggerKeys = await Scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in default group was 1 ");
           triggerKeys.Count.Should().Be(1, "Number of triggers expected in default group was 1 ");

           jobKeys = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
           triggerKeys = await Scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

           jobKeys.Count.Should().Be(2, "Number of jobs expected in 'g1' group was 2 ");
           triggerKeys.Count.Should().Be(2, "Number of triggers expected in 'g1' group was 2 ");
        }

        [Fact]
        public async Task TriggerStateTest()
        {
           await CreateJobsAndTriggers();

           var scheduler = Scheduler;
           var s = await scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");

           await scheduler.PauseTrigger(new TriggerKey("t2", "g1"));
           s = await scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t2 expected to be PAUSED ");

           await scheduler.ResumeTrigger(new TriggerKey("t2", "g1"));
           s = await scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");

           var pausedGroups = await scheduler.GetPausedTriggerGroups();
           (pausedGroups).Should().BeEmpty("Size of paused trigger groups list expected to be 0 ");

           await scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

           // test that adding a trigger to a paused group causes the new trigger to be paused also... 
           var job = JobBuilder.Create<SimpleJob>()
               .WithIdentity("j4", "g1")
               .Build();

           var trigger = TriggerBuilder.Create()
               .WithIdentity("t4", "g1")
               .ForJob(job)
               .StartNow()
               .WithSimpleSchedule(x => x.RepeatForever().WithIntervalInSeconds(5))
               .Build();

           await scheduler.ScheduleJob(job, trigger);

           pausedGroups = await scheduler.GetPausedTriggerGroups();
           pausedGroups.Count.Should().Be(1, "Size of paused trigger groups list expected to be 1 ");

           s = await scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t2 expected to be PAUSED ");

           s = await scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
           s.Equals(TriggerState.Paused).Should().BeTrue("State of trigger t4 expected to be PAUSED ");

           await scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));
           s = await scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t2 expected to be NORMAL ");
           s = await scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
           s.Equals(TriggerState.Normal).Should().BeTrue("State of trigger t4 expected to be NORMAL ");
           pausedGroups = await scheduler.GetPausedTriggerGroups();
           (pausedGroups).Should().BeEmpty("Size of paused trigger groups list expected to be 0 ");
        }
        
        [Fact]
        public async Task TestRetry()
        {
           // Arrange
           var job = JobBuilder.Create<UnreliableJob>()
               .WithIdentity("j1")
               .RequestRecovery(true)
               .Build();

           var trigger = TriggerBuilder.Create()
               .WithIdentity("t1")
               .WithSimpleSchedule(x => x.WithMisfireHandlingInstructionIgnoreMisfires())
               .ForJob(job)
               .StartNow()
               .Build();

           await Scheduler.ScheduleJob(job, trigger);
           
           var scheduler = Scheduler;
           await scheduler.Start();

           for (var i = 0; i < 100; i++)
           {
               if (UnreliableJob.Finished)
               {
                   break;
               }
               
               await Task.Delay(500);
           }
           
           await scheduler.Shutdown(true);
           
           Assert.True(UnreliableJob.Finished);
        }

        [Fact]
        public async Task SchedulingTest()
        {
           await CreateJobsAndTriggers();

           (await Scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk"))).Should().BeFalse("Scheduler should have returned 'false' from attempt to unschedule non-existing trigger. ");

           (await Scheduler.UnscheduleJob(new TriggerKey("t3", "g1"))).Should()
               .BeTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

           var jobKeys = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
           var triggerKeys = await Scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in 'g1' group was 1 ");
           // job should have been deleted also, because it is non-durable
           triggerKeys.Count.Should().Be(1, "Number of triggers expected in 'g1' group was 1 ");

           (await Scheduler.UnscheduleJob(new TriggerKey("t1"))).Should().BeTrue("Scheduler should have returned 'true' from attempt to unschedule existing trigger. ");

           jobKeys = await Scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
           triggerKeys = await Scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

           jobKeys.Count.Should().Be(1, "Number of jobs expected in default group was 1 ");
           // job should have been left in place, because it is non-durable
           (triggerKeys).Should().BeEmpty("Number of triggers expected in default group was 0 ");
        }

        [Fact]
        public async Task SimpleReschedulingTest()
        {
           var job = JobBuilder.Create<SimpleJob>().WithIdentity("job1", "group1").Build();
           var trigger1 = TriggerBuilder.Create()
               .ForJob(job)
               .WithIdentity("trigger1", "group1")
               .StartAt(DateTimeOffset.Now.AddSeconds(30))
               .Build();

           await Scheduler.ScheduleJob(job, trigger1);

           job = await Scheduler.GetJobDetail(job.Key);
           job.Should().NotBeNull();

           var trigger2 = TriggerBuilder.Create()
               .ForJob(job)
               .WithIdentity("trigger1", "group1")
               .StartAt(DateTimeOffset.Now.AddSeconds(60))
               .Build();
           await Scheduler.RescheduleJob(trigger1.Key, trigger2);
           job = await Scheduler.GetJobDetail(job.Key);
           job.Should().NotBeNull();
        }

        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedBefore()
        {
           var jobExecTimestamps = new List<DateTime>();
           var barrier = new Barrier(2);

           var scheduler = Scheduler;
           scheduler.Context.Put(Barrier, barrier);
           scheduler.Context.Put(DateStamps, jobExecTimestamps);
           await scheduler.Start();

           Thread.Yield();

           var job1 = JobBuilder.Create<SimpleJobWithSync>()
               .WithIdentity("job1")
               .Build();

           var trigger1 = TriggerBuilder.Create()
               .ForJob(job1)
               .Build();

           var sTime = DateTime.UtcNow;

           await scheduler.ScheduleJob(job1, trigger1);

           barrier.SignalAndWait(TestTimeout);

           await scheduler.Shutdown(false);

           var fTime = jobExecTimestamps[0];

           (fTime - sTime < TimeSpan.FromMilliseconds(15000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
        }

        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
        {
           var jobExecTimestamps = new List<DateTime>();
           var barrier = new Barrier(2);

           var scheduler = Scheduler;
           await scheduler.Clear();

           scheduler.Context.Put(Barrier, barrier);
           scheduler.Context.Put(DateStamps, jobExecTimestamps);

           await scheduler.Start();

           Thread.Yield();

           var job1 = JobBuilder.Create<SimpleJobWithSync>()
               .WithIdentity("job1").
               StoreDurably().Build();
           await scheduler.AddJob(job1, false);

           var sTime = DateTime.UtcNow;

           await scheduler.TriggerJob(job1.Key);

           barrier.SignalAndWait(TestTimeout);

           await scheduler.Shutdown(false);

           var fTime = jobExecTimestamps[0];

           (fTime - sTime < TimeSpan.FromMilliseconds(15000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
           // This is dangerously subjective!  but what else to do?
        }

        [Fact]
        public async Task TestAbilityToFireImmediatelyWhenStartedAfter()
        {
           var jobExecTimestamps = new List<DateTime>();

           var barrier = new Barrier(2);

           var scheduler = Scheduler;
           scheduler.Context.Put(Barrier, barrier);
           scheduler.Context.Put(DateStamps, jobExecTimestamps);

           var job1 = JobBuilder.Create<SimpleJobWithSync>().WithIdentity("job1").Build();
           var trigger1 = TriggerBuilder.Create().ForJob(job1).Build();

           var sTime = DateTime.UtcNow;

           await Scheduler.ScheduleJob(job1, trigger1);
           await scheduler.Start();

           barrier.SignalAndWait(TestTimeout);

           await scheduler.Shutdown(false);

           var fTime = jobExecTimestamps[0];

           (fTime - sTime < TimeSpan.FromMilliseconds(15000)).Should().BeTrue("Immediate trigger did not fire within a reasonable amount of time.");
           // This is dangerously subjective!  but what else to do?
        }

        [Fact]
        public async Task TestScheduleMultipleTriggersForAJob()
        {
           var job = JobBuilder.Create<SimpleJob>().WithIdentity("job1", "group1").Build();
           var trigger1 = TriggerBuilder.Create()
               .WithIdentity("trigger1", "group1")
               .StartNow()
               .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
               .Build();
           var trigger2 = TriggerBuilder.Create()
               .WithIdentity("trigger2", "group1")
               .StartNow()
               .WithSimpleSchedule(x => x.WithIntervalInSeconds(1).RepeatForever())
               .Build();

           var triggersForJob = (IReadOnlyCollection<ITrigger>)new HashSet<ITrigger>{trigger1, trigger2};

           var scheduler = Scheduler;
           await scheduler.ScheduleJob(job, triggersForJob, true);

           var triggersOfJob = await scheduler.GetTriggersOfJob(job.Key);
           triggersOfJob.Count.Should().Be(2);
           (triggersOfJob.Contains(trigger1)).Should().BeTrue();
           (triggersOfJob.Contains(trigger2)).Should().BeTrue();

           await scheduler.Shutdown(false);
        }

        [Fact]
        public async Task TestDurableStorageFunctions()
        {
            // test basic storage functions of scheduler...

            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            var scheduler = Scheduler;
            (await scheduler.CheckExists(new JobKey("j1"))).Should().BeFalse("Unexpected existence of job named 'j1'.");

            await scheduler.AddJob(job, false);

            (await scheduler.CheckExists(new JobKey("j1"))).Should().BeTrue("Unexpected non-existence of job named 'j1'.");

            var nonDurableJob = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2")
                .Build();

            try
            {
                await scheduler.AddJob(nonDurableJob, false);
                throw new Exception("Storage of non-durable job should not have succeeded.");
            }
            catch (Exception e)
            {
                var expectedException = e as SchedulerException;
                expectedException.Should().NotBeNull();
                (await scheduler.CheckExists(new JobKey("j2"))).Should().BeFalse("Unexpected existence of job named 'j2'.");
            }

            await scheduler.AddJob(nonDurableJob, false, true);

            (await scheduler.CheckExists(new JobKey("j2"))).Should().BeTrue("Unexpected non-existence of job named 'j2'.");
        }

        [Fact]
        public async Task TestShutdownWithoutWaitIsUnclean()
        {
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = Scheduler;
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                await scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    Thread.Sleep(50);
                }
            }
            finally
            {
                await scheduler.Shutdown(false);
            }

            barrier.SignalAndWait(TestTimeout);
        }

        [Fact]
        public async Task TestShutdownWithWaitIsClean()
        {
            var shutdown = false;
            var jobExecTimestamps = new List<DateTime>();
            var barrier = new Barrier(2);
            var scheduler = Scheduler;
            try
            {
                scheduler.Context.Put(Barrier, barrier);
                scheduler.Context.Put(DateStamps, jobExecTimestamps);
                await scheduler.Start();
                var jobName = Guid.NewGuid().ToString();
                await Scheduler.AddJob(JobBuilder.Create<SimpleJobWithSync>().WithIdentity(jobName).StoreDurably().Build(),
                    false);
                await Scheduler.ScheduleJob(TriggerBuilder.Create().ForJob(jobName).StartNow().Build());
                while ((await scheduler.GetCurrentlyExecutingJobs()).Count == 0)
                {
                    await Task.Delay(50);
                }
            }
            finally
            {
                var task = Task.Run(async () =>
                {
                    try
                    {
                        await scheduler.Shutdown(true);
                        shutdown = true;
                    }
                    catch (SchedulerException ex)
                    {
                        throw new Exception("exception: " + ex.Message, ex);
                    }
                });
                await Task.Delay(1000);
                shutdown.Should().BeFalse();
                barrier.SignalAndWait(TestTimeout);
                task.Wait();
            }
        }

        [Fact]
        public async Task SmokeTest()
        {
            await new SmokeTestPerformer().Test(_schedulers, true, true);
        }
        
        [Fact]
        public async Task TestHardJob()
        {
            foreach (var scheduler in _schedulers)
            {
                await scheduler.Start();
            }
            
            var job = JobBuilder.Create<HardJob>()
                .WithIdentity("hardjob")
                .Build();

            var trigger = TriggerBuilder.Create().ForJob(job.Key).StartNow().Build();

            await Scheduler.ScheduleJob(job, trigger);

            await Task.Delay(13 * 60 * 1000);
            
            foreach (var scheduler in _schedulers)
            {
                await scheduler.Shutdown();
            }

            HardJob.TimesExecuted.Should().Be(1);
        }

        private async Task CreateJobsAndTriggers()
        {
            var job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j1")
                .StoreDurably()
                .Build();

            var trigger = TriggerBuilder.Create()
                .WithIdentity("t1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await Scheduler.ScheduleJob(job, trigger);

            job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j2", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t2", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await Scheduler.ScheduleJob(job, trigger);

            job = JobBuilder.Create<SimpleJob>()
                .WithIdentity("j3", "g1")
                .Build();

            trigger = TriggerBuilder.Create()
                .WithIdentity("t3", "g1")
                .ForJob(job)
                .StartNow()
                .WithSimpleSchedule(x => x
                    .RepeatForever()
                    .WithIntervalInSeconds(5))
                .Build();

            await Scheduler.ScheduleJob(job, trigger);
        }
    }
}
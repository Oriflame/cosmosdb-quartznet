﻿using System;
using System.Threading.Tasks;

namespace Quartz.Spi.CosmosDbJobStore.Tests.Jobs
{
    public class SimpleJob : IJob
    {
        public Task Execute(IJobExecutionContext context)
        {
            throw new NotImplementedException();
        }
    }
}

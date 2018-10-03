using System;
using Quartz.Logging;
using Xunit.Abstractions;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public class XunitConsoleLogProvider : ILogProvider
    {
        private readonly ITestOutputHelper output;

        
        public XunitConsoleLogProvider(ITestOutputHelper output)
        {
            this.output = output;
        }

        
        public Logger GetLogger(string name)
        {
            return (level, func, exception, parameters) =>
            {
                if (level >= LogLevel.Debug && func != null)
                {
                    output.WriteLine("[" + DateTime.Now.ToLongTimeString() + "] [" + level + "] " + func(), parameters);
                }
                return true;
            };
        }

        public IDisposable OpenNestedContext(string message)
        {
            throw new NotImplementedException();
        }

        public IDisposable OpenMappedContext(string key, string value)
        {
            throw new NotImplementedException();
        }
    }
}
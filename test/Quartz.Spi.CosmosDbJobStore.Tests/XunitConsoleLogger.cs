using System;
using Common.Logging;
using Common.Logging.Factory;
using Xunit.Abstractions;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public class XunitConsoleLogger : AbstractLogger
    {
        private ITestOutputHelper _output;

        
        public XunitConsoleLogger(ITestOutputHelper output)
        {
            _output = output;
        }

        
        protected override void WriteInternal(LogLevel level, object message, Exception exception)
        {
            _output.WriteLine($"[{DateTime.Now:HH:mm:ss.FFF}] [{level}] {message} {exception}");
        }

        public override bool IsTraceEnabled { get; } = true;
        public override bool IsDebugEnabled { get; } = true;
        public override bool IsErrorEnabled { get; } = true;
        public override bool IsFatalEnabled { get; } = true;
        public override bool IsInfoEnabled { get; } = true;
        public override bool IsWarnEnabled { get; } = true;
    }
}
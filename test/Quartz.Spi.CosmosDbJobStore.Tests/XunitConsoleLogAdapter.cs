using Common.Logging;
using Common.Logging.Simple;
using Xunit.Abstractions;

namespace Quartz.Spi.CosmosDbJobStore.Tests
{
    public class XunitConsoleLogAdapter : AbstractSimpleLoggerFactoryAdapter
    {
        private ITestOutputHelper _output;
        
        
        public XunitConsoleLogAdapter(ITestOutputHelper output) : base(LogLevel.All, true, true, true, "HH:mm:ss.FFF")
        {
            _output = output;
        }

        protected override ILog CreateLogger(string name, LogLevel level, bool showLevel, bool showDateTime, bool showLogName, string dateTimeFormat)
        {
            return new XunitConsoleLogger(_output);
        }
    }
}
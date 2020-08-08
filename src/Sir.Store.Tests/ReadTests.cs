using Microsoft.Extensions.Logging;
using NUnit.Framework;

namespace Sir.Search.Tests
{
    public class ReadTests
    {
        private SessionFactory _sessionFactory;

        [SetUp]
        public void Setup()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("Sir.DbUtil.Program", LogLevel.Debug)
                    .AddConsole()
                    .AddEventLog();
            });

            _sessionFactory = new SessionFactory(
                new KeyValueConfiguration("sir.ini"), 
                new BocModel(), 
                loggerFactory.CreateLogger<SessionFactory>());
        }

        [TearDown]
        public void TearDown()
        {
            _sessionFactory.Dispose();
        }
    }
}
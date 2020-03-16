using Microsoft.Extensions.Logging;

namespace Sir.HttpServer.Features
{
    public class CrawlJobQueue : JobQueue
    {
        public CrawlJobQueue(ILogger<JobQueue> logger) : base(logger)
        {
        }
    }
}
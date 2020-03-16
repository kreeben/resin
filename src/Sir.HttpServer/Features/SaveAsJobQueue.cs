using Microsoft.Extensions.Logging;

namespace Sir.HttpServer.Features
{
    public class SaveAsJobQueue : JobQueue
    {
        public SaveAsJobQueue(ILogger<JobQueue> logger) : base(logger)
        {
        }
    }
}
using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sir.Core;

namespace Sir.HttpServer.Features
{

    public abstract class JobQueue : IDisposable
    {
        private readonly ProducerConsumerQueue<AsyncJob> _queue;
        private readonly ILogger _logger;
        private readonly HashSet<string> _enquedIds;

        public (Uri uri, string title) LastProcessed { get; private set; }

        public JobQueue(
            ILogger<JobQueue> logger)
        {
            _queue = new ProducerConsumerQueue<AsyncJob>(1, DispatchJob);
            _logger = logger;
            _enquedIds = new HashSet<string>();
        }

        public void Enqueue(AsyncJob job)
        {
            if (_enquedIds.Add(job.Id))
            {
                _queue.Enqueue(job);
            }
        }

        private void DispatchJob(AsyncJob job)
        {
            try
            {
                job.Execute();

                _enquedIds.Remove(job.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {job} {ex}");
            }
        }

        public void Dispose()
        {
            _queue.Dispose();
        }
    }
}
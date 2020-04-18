using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sir.Core;

namespace Sir.HttpServer.Features
{

    public class JobQueue : IDisposable
    {
        private readonly ProducerConsumerQueue<AsyncJob> _queue;
        private readonly ILogger _logger;
        private readonly ConcurrentDictionary<string, AsyncJob> _enqueued;

        public JobQueue(
            ILogger<JobQueue> logger)
        {
            _queue = new ProducerConsumerQueue<AsyncJob>(1, DispatchJob);
            _logger = logger;
            _enqueued = new ConcurrentDictionary<string, AsyncJob>();
        }

        public void Enqueue(AsyncJob job)
        {
            if (_enqueued.TryAdd(job.Id, job))
            {
                _queue.Enqueue(job);
            }
        }

        public IDictionary<string, object> GetStatus(string id)
        {
            AsyncJob job;

            if (!_enqueued.TryGetValue(id, out job))
            {
                return null;
            }

            return job.Status;
        }

        private void DispatchJob(AsyncJob job)
        {
            try
            {
                job.Execute();

                _enqueued.Remove(job.Id, out _);
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
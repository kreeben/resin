using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Sir.Core
{
    /// <summary>
    /// Enque items and forget about it. They will be consumed by other threads.
    /// Call ProducerConsumerQueue.Complete() to have consumer threads join main.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class ProducerConsumerQueue<T> : IDisposable
    {
        private BlockingCollection<T> _queue;
        private readonly int _numOfConsumers;
        private readonly Action<T> _consumingAction;
        private Task[] _consumers;
        private bool _started;
        private bool _joining;
        private int _enqueued;

        public int QueueLength { get { return _queue.Count; } }
        public int EnqueuedCount { get { return _enqueued; } }

        public bool IsCompleted { get { return _queue == null || _queue.IsCompleted; } }

        public ProducerConsumerQueue(Action<T> consumingAction, int numOfConsumers = 1)
        {
            if (consumingAction == null)
            {
                throw new ArgumentNullException(nameof(consumingAction));
            }

            _queue = new BlockingCollection<T>();
            _numOfConsumers = numOfConsumers;
            _consumingAction = consumingAction;

            Init();
        }

        private void Init()
        {
            if (_started)
                return;

            _consumers = new Task[_numOfConsumers];

            for (int i = 0; i < _numOfConsumers; i++)
            {
                _consumers[i] = Task.Run(() =>
                {
                    try
                    {
                        while (true)
                        {
                            var item = _queue.Take();

                            _consumingAction(item);
                        }
                    }
                    catch (InvalidOperationException) { }
                });
            }

            _started = true;
        }

        public void Enqueue(T item)
        {
            _queue.Add(item);

            Interlocked.Increment(ref _enqueued);
        }

        public void Join()
        {
            if (_joining || _queue.IsCompleted || !_started)
                return;

            _joining = true;

            _queue.CompleteAdding();
            Task.WaitAll(_consumers);
        }

        public void Dispose()
        {
            Join();
            _queue.Dispose();
        }
    }
}

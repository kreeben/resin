using System;
using System.Collections.Concurrent;
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
        private readonly Func<T, Task> _consumingFunc;
        private Task[] _consumers;
        private bool _started;
        private bool _joining;

        public int Count { get { return _queue.Count; } }

        public bool IsCompleted { get { return _queue == null || _queue.IsCompleted; } }

        public ProducerConsumerQueue(int numOfConsumers, Action<T> consumingAction = null, Func<T, Task> callback = null)
        {
            if (consumingAction == null && callback == null)
            {
                throw new ArgumentNullException(nameof(consumingAction));
            }

            _queue = new BlockingCollection<T>();
            _numOfConsumers = numOfConsumers;
            _consumingAction = consumingAction;
            _consumingFunc = callback;

            Start();
        }

        private void Start()
        {
            if (_started)
                return;

            _consumers = new Task[_numOfConsumers];

            if (_consumingAction != null)
            {
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
            }
            else
            {
                for (int i = 0; i < _numOfConsumers; i++)
                {
                    _consumers[i] = Task.Run(async() => 
                    {
                        try
                        {
                            while (true)
                            {
                                var item = _queue.Take();

                                await _consumingFunc(item);
                            }
                        }
                        catch (InvalidOperationException) { }
                        
                    });
                }
            }

            _started = true;
        }

        public void Enqueue(T item)
        {
            _queue.Add(item);
        }

        public void Join()
        {
            if (_joining || _queue.IsCompleted || !_started)
                return;

            _joining = true;

            _queue.CompleteAdding();
            Task.WaitAll(_consumers);

            _joining = false;
        }

        public void Dispose()
        {
            Join();
            _queue.Dispose();
        }
    }
}

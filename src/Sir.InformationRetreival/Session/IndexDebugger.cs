using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using System.Linq;

namespace Sir
{
    public class IndexDebugger : IDisposable
    {
        private readonly Stopwatch _runTime;
        private readonly Stopwatch _time;
        private readonly int _sampleSize;
        private int _batchNo;
        private volatile int _steps;
        private readonly ILogger _logger;

        public int Steps => _steps;

        public IndexDebugger(ILogger logger, int sampleSize = 1000)
        {
            _sampleSize = sampleSize;
            _time = Stopwatch.StartNew();
            _runTime = Stopwatch.StartNew();
            _logger = logger;
        }

        public void Step<T>(IndexSession<T> indexSession, string message = null)
        {
            _steps++;

            if (_steps % _sampleSize == 0)
            {
                var info = indexSession.GetIndexInfo();
                var t = _time.Elapsed.TotalSeconds;
                var docsPerSecond = (int)(_sampleSize / t);
                var debug = string.Join('\n', info.Info.Select(x => x.ToString()));

                _batchNo++;

                var record = $"\n{_runTime.Elapsed} index session running time\n{_time.Elapsed} batch run time\n{_sampleSize * _batchNo} documents\n{debug}\n{docsPerSecond} docs/s\n{message}";

                _logger.LogInformation(record);
                _time.Restart();
            }
        }

        public void Dispose()
        {
            _logger.LogInformation($"session ran for {_runTime.Elapsed}");
        }
    }

    public class BatchDebugger : IDisposable
    {
        private readonly Stopwatch _runTime;
        private readonly Stopwatch _time;
        private readonly ILogger _logger;
        private readonly string _nameOfOperation;
        private readonly int _sampleSize;
        private int _batchNo;
        private volatile int _steps;

        public int StepCount => _steps;
        public TimeSpan Time => _time.Elapsed;

        public BatchDebugger(string nameOfOperation, ILogger logger, int sampleSize = 1000)
        {
            _sampleSize = sampleSize;
            _runTime = Stopwatch.StartNew();
            _time = Stopwatch.StartNew();
            _logger = logger;
            _nameOfOperation = nameOfOperation;
        }

        public void Step()
        {
            _steps++;

            if (_steps % _sampleSize == 0)
            {
                var t = _time.Elapsed.TotalSeconds;
                var itemsPerSecond = (int)(_sampleSize / t);

                _batchNo++;

                var message = $"\n{_nameOfOperation} run time {_runTime.Elapsed}\n{_time.Elapsed} batch run time\n{_sampleSize * _batchNo} items\n{itemsPerSecond} items/s";

                _logger.LogInformation(message);
                _time.Restart();
            }
        }

        public void Dispose()
        {
            _logger.LogInformation($"session ran for {_runTime.Elapsed}");
        }
    }
}

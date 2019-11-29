using Microsoft.Extensions.Logging;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace Sir.Search
{
    public class DebugIndexSession : IDisposable
    {
        private readonly IndexSession _indexSession;
        private readonly ConcurrentDictionary<long, ConcurrentBag<IVector>> _debugWords;
        private readonly ILogger<DebugIndexSession> _logger;

        public DebugIndexSession(IndexSession indexSession, ILogger<DebugIndexSession> logger)
        {
            _indexSession = indexSession;
            _debugWords = new ConcurrentDictionary<long, ConcurrentBag<IVector>>();
            _logger = logger;
        }

        public void Put(long docId, long keyId, string value)
        {
            var tokens = _indexSession.Model.Tokenize(value);
            var column = _indexSession.Index.GetOrAdd(keyId, new VectorNode());

            foreach (var vector in tokens)
            {
                _indexSession.Put(docId, vector, column);

                _debugWords.GetOrAdd(keyId, new ConcurrentBag<IVector>()).Add(vector);
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return _indexSession.GetIndexInfo();
        }

        private void Debug()
        {
            var debugOutput = new StringBuilder();

            foreach (var column in _indexSession.Index)
            {
                var debugWords = _debugWords[column.Key];
                var wordSet = new HashSet<IVector>();

                foreach (var term in debugWords)
                {
                    if (wordSet.Add(term))
                    {
                        var hit = PathFinder.ClosestMatch(column.Value, term, _indexSession.Model);

                        if (hit != null && hit.Score >= _indexSession.Model.IdenticalAngle)
                        {
                            continue;
                        }

                        throw new Exception($"could not find {term}");
                    }
                }

                debugOutput.AppendLine($"{column.Key}: {wordSet.Count} words");

                foreach (var term in wordSet)
                {
                    debugOutput.AppendLine(term.ToString());
                }
            }

            _logger.LogInformation(debugOutput.ToString());
        }

        public void Dispose()
        {
            _indexSession.Dispose();

            Debug();
        }
    }
}
using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    public class IndexSession<T> : IIndexSession<T>, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexReadWriteStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly ISessionFactory _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly SortedList<int, float> _embedding = new SortedList<int, float>();
        private readonly Dictionary<(string directory, ulong collectionId, long keyId), ColumnReader> _readers;
        private readonly Dictionary<long, Dictionary<(long keyId, long pageId), HashSet<long>>> _postingsToAppend;

        public IndexSession(
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            ISessionFactory sessionFactory, 
            string directory,
            ulong collectionId,
            ILogger logger = null)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
            _sessionFactory = sessionFactory;
            _directory = directory;
            _collectionId = collectionId;
            _logger = logger;
            _readers = new Dictionary<(string, ulong, long), ColumnReader>();
            _postingsToAppend = new Dictionary<long, Dictionary<(long keyId, long pageId), HashSet<long>>>();
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label, _embedding);

            Put(docId, keyId, tokens);
        }

        public void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens)
        {
            var reader = GetColumnReader(keyId);

            if (!_index.TryGetValue(keyId, out var column))
            {
                column = new VectorNode();
                _index.Add(keyId, column);
            }

            foreach (var token in tokens)
            {
                if (reader == null)
                {
                    _indexingStrategy.Put<T>(
                                    column,
                                    new VectorNode(vector: token, docId: docId));
                }
                else
                {
                    var hit = reader.ClosestMatchOrNullStoppingAtFirstIdenticalPage(token);

                    if (hit == null || hit.Score < _model.IdenticalAngle)
                    {
                        _indexingStrategy.Put<T>(
                                    column,
                                    new VectorNode(vector: token, docId: docId));
                    }
                    else
                    {
                        var compositeKey = (keyId, hit.PostingsPageIds[0]);

                        if (!_postingsToAppend.TryGetValue(keyId, out var postingsSet))
                        {
                            postingsSet = new Dictionary<(long keyId, long pageId), HashSet<long>> { { compositeKey, new HashSet<long> { docId } } };
                            _postingsToAppend.Add(keyId, postingsSet);
                        }
                        else
                        {
                            if (!postingsSet.TryGetValue(compositeKey, out var postings))
                            {
                                postingsSet.Add(compositeKey, new HashSet<long> { docId });
                            }
                            else
                            {
                                postings.Add(docId);
                            }
                        }
                        
                    }
                }
            }
        }

        public void Commit()
        {
            foreach (var column in _index)
            {
                Commit(column.Key);
            }

            foreach (var reader in _readers.Values)
            {
                reader.Dispose();
            }

            _index.Clear();
            _postingsToAppend.Clear();
            _readers.Clear();
        }

        private void Commit(long keyId)
        {
            var time = Stopwatch.StartNew();

            var column = _index[keyId];
            _indexingStrategy.Commit(_directory, _collectionId, keyId, column, _sessionFactory, _postingsToAppend.ContainsKey(keyId) ? _postingsToAppend[keyId] : null, _logger);

            if (_logger != null)
                _logger.LogInformation($"committed index to disk for key {keyId} in {time.Elapsed}");
        }

        public IDictionary<long, VectorNode> GetInMemoryIndices()
        {
            return _index;
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }


        private IEnumerable<GraphInfo> GetGraphInfo()
        {
            foreach (var ix in _index)
            {
                yield return new GraphInfo(ix.Key, ix.Value);
            }
        }

        private ColumnReader GetColumnReader(long keyId)
        {
            ColumnReader reader;
            var key = (_directory, _collectionId, keyId);

            if (!_readers.TryGetValue(key, out reader))
            {
                reader = _sessionFactory.CreateColumnReader(_directory, _collectionId, keyId, _model);

                if (reader != null)
                {
                    _readers.Add(key, reader);
                }
            }

            return reader;
        }

        public void Dispose()
        {
            if(_index.Count > 0)
            {
                Commit();
            }

            foreach (var reader in _readers.Values)
            {
                reader.Dispose();
            }
        }
    }
}
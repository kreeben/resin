using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir
{
    /// <summary>
    /// Write a paged index.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class IndexSession<T> :  IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexReadWriteStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly IndexCache _indexCache;

        public IndexSession(
            string directory,
            ulong collectionId,
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            IndexCache indexCache,
            ILogger logger = null)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
            _directory = directory;
            _collectionId = collectionId;
            _logger = logger;
            _indexCache = indexCache;
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label);

            Put(docId, keyId, tokens);
        }

        private void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens)
        {
            VectorNode column;

            if (!_index.TryGetValue(keyId, out column))
            {
                column = new VectorNode();
                _index.Add(keyId, column);
            }

            foreach (var token in tokens)
            {
                if (!token.IsEmptyVector())
                    _indexingStrategy.Put<T>(
                                        column,
                                        new VectorNode(vector:token, docId:docId, keyId:keyId));
            }
        }

        public void Commit()
        {
            foreach (var column in _index)
            {
                Commit(column.Key);
            }
        }

        public void Commit(long keyId)
        {
            var time = Stopwatch.StartNew();

            var column = _index[keyId];

            _indexingStrategy.SerializePage(_directory, _collectionId, keyId, column, _indexCache, _logger);

            if (_logger != null)
                _logger.LogInformation($"committing index to disk for key {keyId} took {time.Elapsed}");

            _index.Remove(keyId);
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

        public void Dispose()
        {
            if(_index.Count > 0)
            {
                Commit();
            }
        }
    }
}
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
        private readonly IStreamDispatcher _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly SortedList<int, float> _embedding = new SortedList<int, float>();

        public IndexSession(
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            IStreamDispatcher sessionFactory, 
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
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label, _embedding);

            Put(docId, keyId, tokens);
        }

        public void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens)
        {
            VectorNode column;

            if (!_index.TryGetValue(keyId, out column))
            {
                column = new VectorNode();
                _index.Add(keyId, column);
            }

            foreach (var token in tokens)
            {
                _indexingStrategy.Put<T>(
                                    column,
                                    new VectorNode(vector: token, docId: docId));
            }
        }

        public void Put(VectorNode token)
        {
            VectorNode column;

            if (!_index.TryGetValue(token.KeyId.Value, out column))
            {
                column = new VectorNode();
                _index.Add(token.KeyId.Value, column);
            }

            foreach (var node in PathFinder.All(token))
            {
                _indexingStrategy.Put<T>(
                    column,
                    new VectorNode(node.Vector, docIds: node.DocIds));
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
            var column = _index[keyId];

            _indexingStrategy.Commit(_directory, _collectionId, keyId, column, _sessionFactory, _logger);
            _index.Remove(keyId);

            Print(keyId, column);
        }

        private static void Print(long keyId, VectorNode tree)
        {
            var diagram = PathFinder.Visualize2(tree);
            System.IO.File.WriteAllText($@"c:\temp\{keyId}.txt", diagram);
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
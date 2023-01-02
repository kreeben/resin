﻿using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;

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
            var documentTree = new VectorNode(keyId: keyId);

            foreach (var token in tokens)
            {
                documentTree.AddIfUnique(new VectorNode(token, docId: docId, keyId: keyId), _model);
            }

            Put(documentTree);
        }

        public void Put(VectorNode documentTree)
        {
            VectorNode column;

            if (!_index.TryGetValue(documentTree.KeyId.Value, out column))
            {
                column = new VectorNode();
                _index.Add(documentTree.KeyId.Value, column);
            }

            foreach (var node in PathFinder.All(documentTree))
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
﻿using System;
using System.Collections.Generic;

namespace Sir.Search
{
    public class InMemoryIndexSession<T> : IIndexSession, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexingStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly IDictionary<long, IColumnReader> _readers;
        private readonly SessionFactory _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;

        public InMemoryIndexSession(
            IModel<T> model,
            IIndexingStrategy indexingStrategy,
            SessionFactory sessionFactory, 
            string directory,
            ulong collectionId)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
            _readers = new Dictionary<long, IColumnReader>();
            _sessionFactory = sessionFactory;
            _directory = directory;
            _collectionId = collectionId;
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label);

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
                _indexingStrategy.ExecutePut<T>(
                    column, 
                    new VectorNode(node.Vector, docIds: node.DocIds), 
                    GetReader(documentTree.KeyId.Value));
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public IDictionary<long, VectorNode> GetInMemoryIndices()
        {
            return _index;
        }

        private IColumnReader GetReader(long keyId)
        {
            IColumnReader reader;

            if (!_readers.TryGetValue(keyId, out reader))
            {
                reader = _sessionFactory.CreateColumnReader(_directory, _collectionId, keyId);
                _readers.Add(keyId, reader);
            }

            return reader;
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
            foreach (var reader in _readers.Values)
                reader.Dispose();
        }
    }
}
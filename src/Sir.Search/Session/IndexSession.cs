﻿using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Sir.Search
{
    public class IndexSession<T> : IIndexSession, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexingStrategy _indexingStrategy;
        private readonly ConcurrentDictionary<long, VectorNode> _index;

        public IndexSession(
            IModel<T> model,
            IIndexingStrategy indexingStrategy)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new ConcurrentDictionary<long, VectorNode>();
        }

        public void Put(long docId, long keyId, T value)
        {
            var tokens = _model.Tokenize(value);

            Put(docId, keyId, tokens);
        }

        public void Put(long docId, long keyId, IEnumerable<IVector> tokens)
        {
            var column = _index.GetOrAdd(keyId, key => new VectorNode());

            foreach (var token in tokens)
            {
                _indexingStrategy.ExecutePut<T>(column, new VectorNode(token, docId));
            }
        }

        public void Put(VectorNode tree)
        {
            var column = _index.GetOrAdd(tree.KeyId.Value, key => new VectorNode());

            foreach (var node in PathFinder.All(tree))
            {
                _indexingStrategy.ExecutePut<T>(column, new VectorNode(node.Vector, docIds: node.DocIds));
            }
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public IDictionary<long, VectorNode> GetInMemoryIndex()
        {
            return _index;
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
        }
    }

    public class EmbeddSession<T> : IIndexSession, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexingStrategy _indexingStrategy;
        private readonly ConcurrentDictionary<long, VectorNode> _index;

        public EmbeddSession(
            IModel<T> model,
            IIndexingStrategy indexingStrategy)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new ConcurrentDictionary<long, VectorNode>();
        }

        public void Put(long docId, long keyId, T value)
        {
            var vectors = _model.Tokenize(value);
            var column = _index.GetOrAdd(keyId, new VectorNode());

            foreach (var vector in vectors)
            {
                _indexingStrategy.ExecutePut<T>(column, new VectorNode(vector, docId));
            }

            var size = PathFinder.Size(column);


        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(GetGraphInfo());
        }

        public IDictionary<long, VectorNode> GetInMemoryIndex()
        {
            return _index;
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
        }
    }
}
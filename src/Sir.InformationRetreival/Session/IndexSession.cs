using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;

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
        private readonly IDictionary<long, VectorNode> _postings;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly IndexIndex _indexIndex;

        public IndexSession(
            string directory,
            ulong collectionId,
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            IndexIndex indexIndex,
            ILogger logger = null)
        {
            _model = model;
            _indexingStrategy = indexingStrategy;
            _index = new Dictionary<long, VectorNode>();
            _postings = new Dictionary<long, VectorNode>();
            _directory = directory;
            _collectionId = collectionId;
            _logger = logger;
            _indexIndex = indexIndex;
        }

        public void Put(long documentId, long keyId, T value, bool label)
        {
            VectorNode index;

            if (!_index.TryGetValue(keyId, out index))
            {
                index = new VectorNode();
                _index.Add(keyId, index);
            }

            foreach (var token in _model.CreateEmbedding(value, label))
            {
                Put(index, documentId, keyId, token);
            }
        }

        private void Put(VectorNode index, long documentId, long keyId, ISerializableVector token)
        {
            VectorNode existingNode = _indexIndex.Get(keyId, token);

            if (existingNode == null)
            {
                _indexingStrategy.Put<T>(index, new VectorNode(vector: token, documentId: documentId, keyId: keyId));
                _indexIndex.Put(new VectorNode(vector: token, keyId: keyId));
            }
            else
            {
                VectorNode postings;

                if (!_postings.TryGetValue(keyId, out postings))
                {
                    postings = new VectorNode();
                    _postings.Add(keyId, postings);
                }

                GraphBuilder.AddOrAppend(postings, new VectorNode(vector: token, documentId: documentId, keyId: keyId), _model);
            }
        }

        public void Commit()
        {
            foreach (var column in _index)
            {
                Commit(column.Key);
            }
        }

        private void Commit(long keyId)
        {
            _indexingStrategy.SerializePage(_directory, _collectionId, keyId, _index[keyId], _postings[keyId], _indexIndex, _logger);
            _index.Remove(keyId);
            _postings.Remove(keyId);
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
            if (_index != null && _index.Count > 0)
            {
                Commit();
            }
        }
    }
}
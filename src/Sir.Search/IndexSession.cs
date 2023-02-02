using MathNet.Numerics.LinearAlgebra;
using MathNet.Numerics.LinearAlgebra.Storage;
using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Sir
{
    public class IndexSession<T> : IIndexSession<T>, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexReadWriteStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly IDictionary<long, SortedList<double, VectorInfo>> _fields;
        private readonly IStreamDispatcher _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly SortedList<int, float> _embedding = new SortedList<int, float>();
        private readonly IDictionary<long, Vector<float>> _meanVectors;
        private readonly StringBuilder _skipLog = new StringBuilder();

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
            _fields = new Dictionary<long, SortedList<double, VectorInfo>>();
            _sessionFactory = sessionFactory;
            _directory = directory;
            _collectionId = collectionId;
            _logger = logger;
            _meanVectors = _sessionFactory.DeserializeMeanVectors(model, directory, collectionId);
        }

        public void PutMany(long docId, long keyId, IEnumerable<ISerializableVector> vectors)
        {
            SortedList<double, VectorInfo> vectorNodes;

            if (!_fields.TryGetValue(keyId, out vectorNodes))
            {
                vectorNodes = new SortedList<double, VectorInfo>();
                _fields.Add(keyId, vectorNodes);
            }

            var meanVector = _meanVectors[keyId];

            foreach (var vector in vectors)
            {
                var angle = meanVector.CosAngle(vector.Value);

                if (angle > 0)
                {
                    VectorInfo vectorNode;

                    if (vectorNodes.TryGetValue(angle, out vectorNode))
                    {
                        vectorNode.DocIds.Add(docId);
                    }
                    else
                    {
                        vectorNode = new VectorInfo { Angle = angle, DocIds = new HashSet<long> { docId }, ComponentCount = ((SparseVectorStorage<float>)vector.Value.Storage).ValueCount };
                        vectorNodes.Add(angle, vectorNode);
                    }
                }
                else
                {
                    _skipLog.AppendLine($"{angle} {vector.Label}");
                }
            }
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
            foreach (var field in _fields)
            {
                Commit(field.Key);
            }
        }

        public void Commit(long keyId)
        {
            var field = _fields[keyId];

            _indexingStrategy.Commit(_directory, _collectionId, keyId, field, _sessionFactory, _logger);
            File.WriteAllText("c:\\temp\\skipped.txt", _skipLog.ToString());
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
using Sir.IO;
using System;
using System.Collections.Generic;

namespace Sir
{
    public class IndexCache
    {
        private readonly IModel _model;
        private readonly IDictionary<long, VectorNode> _cache; // indices by key ID

        public IndexCache(IModel model)
        {
            _model = model;
            _cache = new Dictionary<long, VectorNode>();
        }

        public void Put(VectorNode node)
        {
            if (!node.KeyId.HasValue)
                throw new ArgumentException(message:"VectorNode does not have a key ID.", paramName:nameof(node));

            if (!_cache.TryGetValue(node.KeyId.Value, out var tree))
            {
                tree = new VectorNode();
                _cache.Add(node.KeyId.Value, tree);
            }

            GraphBuilder.AddOrAppend(tree, node, _model);
        }

        public long? GetPostingsOffset(long keyId, ISerializableVector vector)
        {
            if (_cache.TryGetValue(keyId, out var tree))
            {
                var hit = PathFinder.ClosestMatch(tree, vector, _model);

                if (hit.Score.Approximates(_model.IdenticalAngle))
                {
                    return hit.Node.PostingsOffset == -1 ? null : hit.Node.PostingsOffset;
                }
            }
            return null;
        }

        public void UpdatePostingsOffset(long keyId, ISerializableVector vector, long postingsOffset)
        {
            if (_cache.TryGetValue(keyId, out var tree))
            {
                var hit = PathFinder.ClosestMatch(tree, vector, _model);

                if (hit.Score.Approximates(_model.IdenticalAngle))
                {
                    hit.Node.PostingsOffset = postingsOffset;
                }
            }
        }
    }
}
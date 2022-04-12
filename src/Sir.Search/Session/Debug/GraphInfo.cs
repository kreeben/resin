﻿namespace Sir.Search
{
    public class GraphInfo
    {
        private readonly long _keyId;
        private readonly VectorNode _graph;

        public GraphInfo(long keyId, VectorNode graph)
        {
            _keyId = keyId;
            _graph = graph;
        }

        public override string ToString()
        {
            return $"key {_keyId} weight {_graph.Weight} {PathFinder.Size(_graph)}";
        }
    }
}
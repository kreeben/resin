﻿using Sir.Core;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Store
{
    /// <summary>
    /// Indexing session targeting a single collection.
    /// </summary>
    public class TermIndexSession : CollectionSession, IDisposable, ILogger
    {
        private readonly IConfigurationProvider _config;
        private readonly IStringModel _model;
        private readonly ConcurrentDictionary<long, ConcurrentDictionary<int, VectorNode>> _level3Index;
        //private readonly ConcurrentDictionary<long, ConcurrentDictionary<int, VectorNode>> _level2Index;
        private readonly ConcurrentDictionary<long, VectorNode> _level1Index;
        private readonly ProducerConsumerQueue<(long docId, long keyId, IVector term)> _level1Workers;
        //private readonly ProducerConsumerQueue<(int indexId, long docId, long keyId, IVector term)> _level2Workers;
        private readonly ProducerConsumerQueue<(int indexId, long docId, long keyId, IVector term)> _level3Workers;
        private Stream _postingsStream;
        private readonly Stream _vectorStream;

        public TermIndexSession(
            ulong collectionId,
            SessionFactory sessionFactory, 
            IStringModel model,
            IConfigurationProvider config) : base(collectionId, sessionFactory)
        {
            _config = config;
            _model = model;
            _level3Index = new ConcurrentDictionary<long, ConcurrentDictionary<int, VectorNode>>();
            //_level2Index = new ConcurrentDictionary<long, ConcurrentDictionary<int, VectorNode>>();
            _level1Index = new ConcurrentDictionary<long, VectorNode>();
            _postingsStream = SessionFactory.CreateAppendStream(Path.Combine(SessionFactory.Dir, $"{CollectionId}.pos"));
            _vectorStream = SessionFactory.CreateAppendStream(Path.Combine(SessionFactory.Dir, $"{CollectionId}.vec"));
            _level1Workers = new ProducerConsumerQueue<(long, long, IVector)>(int.Parse(config.Get("level1_index_session_thread_count")), PutLevel1);
            //_level2Workers = new ProducerConsumerQueue<(int, long, long, IVector)>(int.Parse(config.Get("level2_index_session_thread_count")), PutLevel2);
            _level3Workers = new ProducerConsumerQueue<(int indexId, long docId, long keyId, IVector term)>(int.Parse(config.Get("level3_index_session_thread_count")), PutLevel3);
        }

        public void Put(long docId, long keyId, string value)
        {
            var terms = _model.Tokenize(value).Embeddings;

            foreach (var term in terms)
            {
                _level1Workers.Enqueue((docId, keyId, term));
            }
        }

        public void PutLevel1((long docId, long keyId, IVector term) workItem)
        {
            var level1Index = GetOrCreateLevel1Index(workItem.keyId);

            VectorNode parent;
            long indexId;
            bool left;
            var node = new VectorNode(workItem.term, workItem.docId);

            if (!GraphBuilder.TryMerge(level1Index, node, _model.Level1IdenticalAngle, _model.Level1FoldAngle, _model, out parent, out left))
            {
                node.PostingsOffset = indexId = level1Index.Weight;

                if (left)
                {
                    parent.Left = node;
                }
                else
                {
                    parent.Right = node;
                }
            }
            else
            {
                indexId = parent.PostingsOffset;
            }

            _level3Workers.Enqueue(((int)indexId, workItem.docId, workItem.keyId, workItem.term));
        }

        //private void PutLevel2((int indexId, long docId, long keyId, IVector term) workItem)
        //{
        //    VectorNode level2Index = GetOrCreateLevel2Index(workItem.keyId, workItem.indexId);
        //    long indexId;
        //    var node = new VectorNode(workItem.term, workItem.docId);
        //    VectorNode parent;
        //    bool left;

        //    if (!GraphBuilder.TryMerge(level2Index, node, _model.Level2IdenticalAngle, _model.Level2FoldAngle, _model, out parent, out left))
        //    {
        //        node.PostingsOffset = indexId = level2Index.Weight;

        //        if (left)
        //        {
        //            parent.Left = node;
        //        }
        //        else
        //        {
        //            parent.Right = node;
        //        }
        //    }
        //    else
        //    {
        //        indexId = parent.PostingsOffset;
        //    }

        //    _level3Workers.Enqueue(((int)indexId, workItem.docId, workItem.keyId, workItem.term));
        //}

        private void PutLevel3((int indexId, long docId, long keyId, IVector term) workItem)
        {
            VectorNode level3Index = GetOrCreateLevel3Index(workItem.keyId, workItem.indexId);
            var node = new VectorNode(workItem.term, workItem.docId);
            VectorNode parent;
            bool left;

            if (!GraphBuilder.TryMerge(level3Index, node, _model.Level3IdenticalAngle, _model.Level3FoldAngle, _model, out parent, out left))
            {
                if (left)
                {
                    parent.Left = node;
                }
                else
                {
                    parent.Right = node;
                }
            }
            else
            {
                GraphBuilder.AddDocId(parent, workItem.docId);
            }
        }

        private VectorNode GetOrCreateLevel3Index(long keyId, int indexId)
        {
            var list = _level3Index.GetOrAdd(keyId, new ConcurrentDictionary<int, VectorNode>());
            var ix = list.GetOrAdd(indexId, new VectorNode());

            return list[indexId];
        }

        //private VectorNode GetOrCreateLevel2Index(long keyId, int indexId)
        //{
        //    var list = _level2Index.GetOrAdd(keyId, new ConcurrentDictionary<int, VectorNode>());
        //    var ix = list.GetOrAdd(indexId, new VectorNode());

        //    return list[indexId];
        //}

        private VectorNode GetOrCreateLevel1Index(long keyId)
        {
            return _level1Index.GetOrAdd(keyId, new VectorNode());
        }

        public IndexInfo GetIndexInfo()
        {
            return new IndexInfo(_level1Workers.Count, _level3Workers.Count, GetLevel1Info(), GetLevel3Info());
        }

        private IEnumerable<GraphInfo> GetLevel1Info()
        {
            foreach (var node in _level1Index)
            {
                yield return new GraphInfo("level1", node.Key, node.Value);
            }
        }

        //private IEnumerable<GraphInfo> GetLevel2Info()
        //{
        //    foreach (var list in _level2Index)
        //    {
        //        var i = 0;

        //        foreach (var node in list.Value.Values)
        //            yield return new GraphInfo($"level2 p-{i++}", list.Key, node);
        //    }
        //}

        private IEnumerable<GraphInfo> GetLevel3Info()
        {
            foreach (var list in _level3Index)
            {
                var i = 0;

                foreach (var node in list.Value.Values)
                    yield return new GraphInfo($"level3 p-{i++}", list.Key, node);
            }
        }

        private void Serialize()
        {
            foreach (var column in _level1Index)
            {
                using (var writer = new ColumnWriter(CollectionId, column.Key, SessionFactory, "ix1"))
                {
                    writer.CreatePage(column.Value, _vectorStream, _postingsStream, _model);
                }
            }

            //foreach (var column in _level2Index)
            //{
            //    using (var writer = new ColumnWriter(CollectionId, column.Key, SessionFactory, "ix2"))
            //    {
            //        foreach (var node in new SortedList<int, VectorNode>(column.Value).Values)
            //        {
            //            writer.CreatePage(node, _vectorStream, _postingsStream, _model);
            //        }
            //    }
            //}

            foreach (var column in _level3Index)
            {
                using (var writer = new ColumnWriter(CollectionId, column.Key, SessionFactory, "ix3"))
                {
                    foreach (var node in new SortedList<int, VectorNode>(column.Value).Values)
                    {
                        writer.CreatePage(node, _vectorStream, _postingsStream, _model);
                    }
                }
            }
        }

        public void Dispose()
        {
            var time = Stopwatch.StartNew();

            this.Log("synchronizing");

            _level1Workers.Dispose();
            //_level2Workers.Dispose();
            _level3Workers.Dispose();

            this.Log($"synchronization took {time.Elapsed}");

            Serialize();

            _postingsStream.Dispose();
            _vectorStream.Dispose();

            SessionFactory.ClearPageInfo();
        }

        //private void Validate((long keyId, long docId, AnalyzedData tokens) item)
        //{
        //    var tree = GetOrCreateSecondaryIndex(item.keyId);

        //    foreach (var vector in item.tokens.Embeddings)
        //    {
        //        var hit = PathFinder.ClosestMatch(tree, vector, _model);

        //        if (hit.Score < _model.IdenticalAngle)
        //        {
        //            throw new DataMisalignedException();
        //        }

        //        var valid = false;

        //        foreach (var id in hit.Node.DocIds)
        //        {
        //            if (id == item.docId)
        //            {
        //                valid = true;
        //                break;
        //            }
        //        }

        //        if (!valid)
        //        {
        //            throw new DataMisalignedException();
        //        }
        //    }
        //}
    }

    public class IndexInfo
    {
        public int L1QueueLength { get; }
        public int L2QueueLength { get; }
        public int L3QueueLength { get; }

        public IEnumerable<GraphInfo> Level1Info { get; }
        public IEnumerable<GraphInfo> Level2Info { get; }
        public IEnumerable<GraphInfo> Level3Info { get; }

        public IndexInfo(int level1Queue, int level3Queue, IEnumerable<GraphInfo> level1Stats, IEnumerable<GraphInfo> level3Stats)
        {
            L1QueueLength = level1Queue;
            L3QueueLength = level3Queue;
            Level3Info = level3Stats;
            Level1Info = level1Stats;
        }
    }

    public class GraphInfo
    {
        private readonly string _name;
        private readonly long _keyId;
        private readonly VectorNode _graph;

        public long Weight => _graph.Weight;

        public GraphInfo(string name, long keyId, VectorNode graph)
        {
            _name = name;
            _keyId = keyId;
            _graph = graph;
        }

        public override string ToString()
        {
            return $"{_name} key: {_keyId} weight: {_graph.Weight} depth/width: {PathFinder.Size(_graph)}";
        }
    }
}
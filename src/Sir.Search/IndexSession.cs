using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir
{
    public class IndexSession<T> : IIndexSession<T>, IDisposable
    {
        private readonly IModel<T> _model;
        private readonly IIndexReadWriteStrategy _indexingStrategy;
        private readonly IDictionary<long, VectorNode> _index;
        private readonly IDictionary<long, IList<VectorNode>> _columns;
        private readonly SessionFactory _sessionFactory;
        private readonly string _directory;
        private readonly ulong _collectionId;
        private readonly ILogger _logger;
        private readonly SortedList<int, float> _embedding = new SortedList<int, float>();
        private readonly Dictionary<long, Dictionary<(long keyId, long pageId), HashSet<long>>> _postingsToAppend;

        public IndexSession(
            IModel<T> model,
            IIndexReadWriteStrategy indexingStrategy,
            SessionFactory sessionFactory,
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
            _postingsToAppend = new Dictionary<long, Dictionary<(long keyId, long pageId), HashSet<long>>>();
            _columns = sessionFactory.GetColumns(directory, collectionId, model);
        }

        public void Put(long docId, long keyId, T value, bool label)
        {
            var tokens = _model.CreateEmbedding(value, label, _embedding);

            Put(docId, keyId, tokens);
        }

        public void Put(long docId, long keyId, IEnumerable<ISerializableVector> tokens)
        {
            if (!_index.TryGetValue(keyId, out var column))
            {
                column = new VectorNode();
                _index.Add(keyId, column);
            }

            foreach (var token in tokens)
            {
                var hit = FindInExistingPages(keyId, token);

                if (hit == null || hit.Score < _model.IdenticalAngle)
                {
                    _indexingStrategy.Put<T>(
                                column,
                                new VectorNode(vector: token, docId: docId));
                }
                else
                {
                    var compositeKey = (keyId, hit.Node.PostingsPageId);

                    if (!_postingsToAppend.TryGetValue(keyId, out var postingsSet))
                    {
                        postingsSet = new Dictionary<(long keyId, long pageId), HashSet<long>> { { compositeKey, new HashSet<long> { docId } } };
                        _postingsToAppend.Add(keyId, postingsSet);
                    }
                    else
                    {
                        if (!postingsSet.TryGetValue(compositeKey, out var postings))
                        {
                            postingsSet.Add(compositeKey, new HashSet<long> { docId });
                        }
                        else
                        {
                            postings.Add(docId);
                        }
                    }
                }
            }
        }

        private Hit FindInExistingPages(long keyId, ISerializableVector vector)
        {
            if (_columns.TryGetValue(keyId, out var column))
            {
                Hit best = null;

                foreach (var page in column)
                {
                    var hit = PathFinder.ClosestMatch(page, vector, _model);

                    if (best == null || (hit != null && hit.Score > best.Score))
                    {
                        best = hit;

                        if (best.Score >= _model.IdenticalAngle)
                            break;
                    }
                }

                return best;
            }
            else
            {
                return null;
            }
        }

        public void Commit()
        {
            foreach (var column in _index)
            {
                using (var indexingWriteStream = _sessionFactory.CreateIndexingWriteStream(_directory, _collectionId, column.Key))
                {
                    Commit(column.Key, indexingWriteStream);
                }
            }

            _index.Clear();
            _postingsToAppend.Clear();
        }

        public void Commit(long keyId, IndexWriteStream indexWriteStream)
        {
            var time = Stopwatch.StartNew();
            var page = _index[keyId];
            var postings = _postingsToAppend.ContainsKey(keyId) ? _postingsToAppend[keyId] : null;
            var size = indexWriteStream.CreatePage(page, postings);

            if (_columns.TryGetValue(keyId, out var pages))
            {
                pages.Add(page);
            }
            else
            {
                _columns.Add(keyId, new List<VectorNode> { page });
            }

            if (_logger != null)
                _logger.LogDebug($"serialized column {keyId}, weight {page.Weight} {size} in {time.Elapsed}");
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
        }
    }

    public class IndexWriteStream : IDisposable
    {
        public Stream IndexStream { get; }
        public Stream VectorStream { get; }
        public Stream PostingsStream { get; }
        public Stream PostingsIndexAppendStream { get; }
        public Stream PostingsIndexUpdateStream { get; }
        public Stream PostingsIndexReadStream { get; }
        public Stream PageIndexStream { get; }

        public PostingsWriter PostingsWriter { get; }
        public ColumnWriter ColumnWriter { get; }
        public PageIndexWriter PageIndexWriter { get; }

        private readonly bool _keepOpen;

        public IndexWriteStream(bool keepOpen = true) : this(
                indexStream: new MemoryStream(),
                vectorStream: new MemoryStream(),
                postingsStream: new MemoryStream(),
                pageIndexStream: new MemoryStream(),
                postingsIndexReadStream: new MemoryStream(),
                postingsIndexUpdateStream: new MemoryStream(),
                postingsIndexAppendStream: new MemoryStream(),
                keepOpen: keepOpen)
        { }

        public IndexWriteStream(Stream indexStream, Stream vectorStream, Stream postingsStream, Stream pageIndexStream, Stream postingsIndexReadStream, Stream postingsIndexUpdateStream, Stream postingsIndexAppendStream, bool keepOpen = false)
        {
            PageIndexStream = pageIndexStream;
            PostingsIndexReadStream = postingsIndexReadStream;
            PostingsIndexUpdateStream = postingsIndexUpdateStream;
            PostingsIndexAppendStream = postingsIndexAppendStream;
            PostingsStream = postingsStream;
            VectorStream = vectorStream;
            IndexStream = indexStream;

            PostingsWriter = new PostingsWriter(
                        PostingsStream,
                        new PostingsIndexAppender(PostingsIndexAppendStream),
                        new PostingsIndexUpdater(PostingsIndexUpdateStream),
                        new PostingsIndexReader(PostingsIndexReadStream));

            ColumnWriter = new ColumnWriter(IndexStream);

            PageIndexWriter = new PageIndexWriter(PageIndexStream);

            _keepOpen = keepOpen;
        }

        public (int depth, int width) CreatePage(VectorNode page, Dictionary<(long keyId, long pageId), HashSet<long>> postings)
        {
            return ColumnWriter.CreatePage(page, VectorStream, PostingsWriter, PageIndexWriter, postings);
        }

        public void Dispose()
        {
            if (_keepOpen)
                return;

            if (IndexStream != null)
            {
                IndexStream.Dispose();
            }
            if (VectorStream != null)
            {
                VectorStream.Dispose();
            }
            if (PostingsIndexAppendStream != null)
            {
                PostingsIndexAppendStream.Dispose();
            }
            if (PostingsIndexUpdateStream != null)
            {
                PostingsIndexUpdateStream.Dispose();
            }
            if (PostingsIndexReadStream != null)
            {
                PostingsIndexReadStream.Dispose();
            }
            if (PageIndexStream != null)
            {
                PageIndexStream.Dispose();
            }
        }
    }
}
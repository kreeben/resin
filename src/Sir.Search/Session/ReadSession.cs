using Microsoft.Extensions.Logging;
using Sir.Document;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir.Search
{
    /// <summary>
    /// Read session targeting a single collection.
    /// </summary>
    public class ReadSession : IDisposable, IReadSession
    {
        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;
        private readonly IStringModel _model;
        private readonly IPostingsReader _postingsReader;
        private readonly ConcurrentDictionary<ulong, DocumentReader> _streamReaders;
        private readonly ConcurrentDictionary<ulong, ConcurrentDictionary<long, INodeReader>> _nodeReaders;
        private readonly ILogger<ReadSession> _logger;

        public ReadSession(
            SessionFactory sessionFactory,
            IConfigurationProvider config,
            IStringModel model,
            IPostingsReader postingsReader,
            ILogger<ReadSession> logger)
        {
            _sessionFactory = sessionFactory;
            _config = config;
            _model = model;
            _streamReaders = new ConcurrentDictionary<ulong, DocumentReader>();
            _postingsReader = postingsReader;
            _nodeReaders = new ConcurrentDictionary<ulong, ConcurrentDictionary<long, INodeReader>>();
            _logger = logger;
        }

        public void Dispose()
        {
            foreach (var reader in _streamReaders.Values)
            {
                reader.Dispose();
            }

            foreach (var collection in _nodeReaders.Values)
                foreach (var reader in collection.Values)
                    reader.Dispose();

            _postingsReader.Dispose();
        }

        public ReadResult Read(IQuery query, int skip, int take)
        {
            if (query is Query q)
                return Read(q, skip, take);

            return Read((Join)query, skip, take);
        }

        private ReadResult Read(Query query, int skip, int take)
        {
            if (query is null)
            {
                throw new ArgumentNullException(nameof(query));
            }

            var result = MapReduceSort(query, skip, take);

            if (result != null)
            {
                var scoreDivider = query.GetDivider();
                var docs = ReadDocs(result.SortedDocuments, scoreDivider);

                return new ReadResult { Query = query, Total = result.Total, Docs = docs };
            }

            return new ReadResult { Query = query, Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        private ReadResult Read(Join join, int skip, int take)
        {
            if (join is null)
            {
                throw new ArgumentNullException(nameof(join));
            }

            var result = MapReduceSort(join.Q, skip, take);

            if (result != null)
            {
                var scoreDivider = join.Q.GetDivider();
                var docs = ReadDocs(result.SortedDocuments, scoreDivider, join.PrimaryKey);
                var primaryKeys = new List<Term>();
                var joinCollectionId = join.Collection.ToHash();
                var primaryKeyId = join.PrimaryKey.ToHash();
                var keyId = _sessionFactory.GetKeyId(joinCollectionId, primaryKeyId);
                Query lookupQuery = null;
                Query cursor = null;

                foreach (var doc in docs)
                {
                    var docTerms = new List<Term>();
                    var tokens = _model.Tokenize((string)doc.Key);

                    foreach (var token in tokens)
                    {
                        docTerms.Add(new Term(
                            joinCollectionId,
                            keyId,
                            join.PrimaryKey,
                            token,
                            and: true,
                            or: false,
                            not: false));
                    }

                    var docQuery = new Query(docTerms, and: false, or: true, not: false);

                    if (lookupQuery == null)
                    {
                        cursor = lookupQuery = docQuery;
                    }
                    else
                    {
                        cursor.Or = docQuery;
                        cursor = docQuery;
                    }
                }

                var secondResult = Read(lookupQuery, 0, int.MaxValue);
                var merged = Merge(docs, secondResult.Docs, join.PrimaryKey);

                return new ReadResult { Query = join.Q, Total = result.Total, Docs = merged };
            }

            return new ReadResult { Query = join.Q, Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        private IEnumerable<IDictionary<string, object>> Merge(
            IDictionary<object,IDictionary<string, object>> first,
            IEnumerable<IDictionary<string, object>> other,
            string primaryKey)
        {
            foreach (var record in other)
            {
                IDictionary<string, object> doc;
                string key = (string)record[primaryKey];

                if (first.TryGetValue(key, out doc))
                {
                    foreach (var field in record)
                    {
                        if (!doc.ContainsKey(field.Key))
                        {
                            doc[field.Key] = field.Value;
                        }
                    }

                    yield return doc;
                }
            }
        }

        public void EnsureIsValid(Query query, long docId)
        {
            foreach (var term in query.Terms)
            {
                var indexReader = GetOrTryCreateIndexReader(term.CollectionId, term.KeyId);

                if (indexReader != null)
                {
                    var hit = indexReader.ClosestTerm(term.Vector, _model);

                    if (hit == null || hit.Score < _model.IdenticalAngle)
                    {
                        throw new DataMisalignedException($"\"{term}\" not found.");
                    }

                    var docIds = _postingsReader
                        .ReadWithPredefinedScore(term.CollectionId, hit.Node.PostingsOffsets, _model.IdenticalAngle);

                    if (!docIds.ContainsKey((term.CollectionId, docId)))
                    {
                        throw new DataMisalignedException(
                            $"document {docId} not found in postings list for term \"{term}\".");
                    }
                }
            }
        }

        private ScoredResult MapReduceSort(Query query, int skip, int take)
        {
            var timer = Stopwatch.StartNew();

            Map(query);

            _logger.LogInformation($"mapping took {timer.Elapsed}");

            timer.Restart();

            var scoredResult = new Dictionary<(ulong, long), double>();

            _postingsReader.Reduce(query, scoredResult);

            _logger.LogInformation("reducing took {0}", timer.Elapsed);

            timer.Restart();

            var sorted = Sort(scoredResult, skip, take);

            _logger.LogInformation("sorting took {0}", timer.Elapsed);

            return sorted;
        }

        /// <summary>
        /// Map query terms to posting list locations.
        /// </summary>
        public void Map(Query query)
        {
            if (query == null)
                return;

            //Parallel.ForEach(query.Terms, term =>
            foreach (var term in query.Terms)
            {
                var indexReader = GetOrTryCreateIndexReader(term.CollectionId, term.KeyId);

                if (indexReader != null)
                {
                    var hit = indexReader.ClosestTerm(term.Vector, _model);

                    if (hit != null && hit.Score > 0)
                    {
                        term.Score = hit.Score;
                        term.PostingsOffsets = hit.Node.PostingsOffsets;
                    }
                }
            }//);

            Map(query.And);
            Map(query.Or);
            Map(query.Not);
        }

        private static ScoredResult Sort(IDictionary<(ulong, long), double> documents, int skip, int take)
        {
            var sortedByScore = new List<KeyValuePair<(ulong, long), double>>(documents);

            sortedByScore.Sort(
                delegate (KeyValuePair<(ulong, long), double> pair1,
                KeyValuePair<(ulong, long), double> pair2)
                {
                    return pair2.Value.CompareTo(pair1.Value);
                }
            );

            var index = skip > 0 ? skip : 0;
            var count = Math.Min(sortedByScore.Count, take > 0 ? take : sortedByScore.Count);

            return new ScoredResult { SortedDocuments = sortedByScore.GetRange(index, count), Total = sortedByScore.Count };
        }

        public INodeReader GetOrTryCreateIndexReader(ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            var collectionReaders = _nodeReaders.GetOrAdd(collectionId, new ConcurrentDictionary<long, INodeReader>());

            return collectionReaders.GetOrAdd(keyId, new NodeReader(
                    collectionId,
                    keyId,
                    _sessionFactory,
                    _sessionFactory.CreateReadStream(Path.Combine(_sessionFactory.Dir, $"{collectionId}.vec")),
                    _sessionFactory.CreateReadStream(ixFileName)));
        }

        public INodeReader GetOrTryCreateIndexReaderNoCache(ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            return new NodeReader(
                    collectionId,
                    keyId,
                    _sessionFactory,
                    _sessionFactory.CreateReadStream(Path.Combine(_sessionFactory.Dir, $"{collectionId}.vec")),
                    _sessionFactory.CreateReadStream(ixFileName));
        }

        public DocumentReader GetOrCreateDocumentReader(ulong collectionId)
        {
            return _streamReaders.GetOrAdd(
                collectionId,
                new DocumentReader(collectionId, _sessionFactory)
                );
        }

        public IList<IDictionary<string, object>> ReadDocs(
            IEnumerable<KeyValuePair<(ulong collectionId, long docId), double>> docIds,
            int scoreDivider)
        {
            var result = new List<IDictionary<string, object>>();

            foreach (var dkvp in docIds)
            {
                var streamReader = GetOrCreateDocumentReader(dkvp.Key.collectionId);
                var docInfo = streamReader.GetDocumentAddress(dkvp.Key.docId);
                var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
                var doc = new Dictionary<string, object>();

                for (int i = 0; i < docMap.Count; i++)
                {
                    var kvp = docMap[i];
                    var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);
                    var key = streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);
                    var val = streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key.ToString()] = val;
                }

                doc["___docid"] = dkvp.Key.docId;
                doc["___score"] = (dkvp.Value / scoreDivider) * 100;

                result.Add(doc);
            }

            return result;
        }

        public IDictionary<object,IDictionary<string, object>> ReadDocs(
            IEnumerable<KeyValuePair<(ulong collectionId, long docId), double>> docIds,
            int scoreDivider,
            string primaryKey)
        {
            var result = new Dictionary<object, IDictionary<string, object>>();

            foreach (var dkvp in docIds)
            {
                var streamReader = GetOrCreateDocumentReader(dkvp.Key.collectionId);
                var docInfo = streamReader.GetDocumentAddress(dkvp.Key.docId);
                var docMap = streamReader.GetDocumentMap(docInfo.offset, docInfo.length);
                var doc = new Dictionary<string, object>();

                for (int i = 0; i < docMap.Count; i++)
                {
                    var kvp = docMap[i];
                    var kInfo = streamReader.GetAddressOfKey(kvp.keyId);
                    var vInfo = streamReader.GetAddressOfValue(kvp.valId);
                    var key = streamReader.GetKey(kInfo.offset, kInfo.len, kInfo.dataType);
                    var val = streamReader.GetValue(vInfo.offset, vInfo.len, vInfo.dataType);

                    doc[key.ToString()] = val;
                }

                doc["___docid"] = dkvp.Key.docId;
                doc["___score"] = (dkvp.Value / scoreDivider) * 100;

                result.Add(doc[primaryKey], doc);
            }

            return result;
        }
    }
}

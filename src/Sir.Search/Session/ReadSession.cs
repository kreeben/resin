using Microsoft.Extensions.Logging;
using Sir.Document;
using Sir.VectorSpace;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Sir.Search
{
    /// <summary>
    /// Read session targeting a single collection.
    /// </summary>
    public class ReadSession : DocumentStreamSession, IDisposable, IReadSession //TODO: rename to QuerySession.
    {
        private readonly SessionFactory _sessionFactory;
        private readonly IConfigurationProvider _config;
        private readonly IStringModel _model;
        private readonly IPostingsReader _postingsReader;
        private readonly ConcurrentDictionary<ulong, DocumentReader> _streamReaders;

        private readonly ILogger<ReadSession> _logger;

        public ReadSession(
            SessionFactory sessionFactory,
            IConfigurationProvider config,
            IStringModel model,
            IPostingsReader postingsReader,
            ILogger<ReadSession> logger) : base(sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _config = config;
            _model = model;
            _streamReaders = new ConcurrentDictionary<ulong, DocumentReader>();
            _postingsReader = postingsReader;
            _logger = logger;
        }

        public ReadResult Read(IQuery query, int skip, int take, string primaryKey = "url")
        {
            var result = MapReduceSort(query, skip, take);

            if (result != null)
            {
                var docs = ReadDocs(result.SortedDocuments, primaryKey, query.Select);

                return new ReadResult { Query = query, Total = result.Total, Docs = docs };
            }

            return new ReadResult { Query = query, Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        private ScoredResult MapReduceSort(IQuery query, int skip, int take)
        {
            var timer = Stopwatch.StartNew();

            // Map
            Map(query);

            _logger.LogInformation($"mapping took {timer.Elapsed}");
            timer.Restart();

            // Reduce
            IDictionary<(ulong, long), double> scoredResult = new Dictionary<(ulong, long), double>();
            _postingsReader.Reduce(query, query.TotalNumberOfTerms(),ref  scoredResult);

            _logger.LogInformation("reducing took {0}", timer.Elapsed);
            timer.Restart();

            // Sort
            var sorted = Sort(scoredResult, skip, take);

            _logger.LogInformation("sorting took {0}", timer.Elapsed);

            return sorted;
        }

        /// <summary>
        /// Map query terms to posting list locations.
        /// </summary>
        private void Map(IQuery query)
        {
            if (query == null)
                return;

            //foreach (var q in query.All())
            Parallel.ForEach(query.All(), q =>
            {
                //foreach (var term in q.Terms)
                Parallel.ForEach(q.Terms, term =>
                {
                    var columnReader = CreateColumnReader(term.CollectionId, term.KeyId);

                    if (columnReader != null)
                    {
                        using (columnReader)
                        {
                            var hit = columnReader.ClosestMatch(term.Vector, _model);

                            if (hit != null)
                            {
                                term.Score = hit.Score;
                                term.PostingsOffsets = hit.Node.PostingsOffsets;
                            }
                        }
                    }
                });
            });
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
            int count;

            if (take == 0)
                count = sortedByScore.Count - (index);
            else
                count = Math.Min(sortedByScore.Count - (index), take);

            return new ScoredResult 
            { 
                SortedDocuments = sortedByScore.GetRange(index, count), 
                Total = sortedByScore.Count 
            };
        }

        private IList<IDictionary<string, object>> ReadDocs(
            IEnumerable<KeyValuePair<(ulong collectionId, long docId), double>> docIds,
            string primaryKey,
            HashSet<string> select)
        {
            if (!select.Contains(primaryKey))
            {
                select.Add(primaryKey);
            }

            var result = new List<IDictionary<string, object>>();
            var documentsByPrimaryKey = new Dictionary<ulong, IDictionary<string, object>>();
            var timer = Stopwatch.StartNew();

            foreach (var d in docIds)
            {
                var doc = ReadDoc(d.Key, select, d.Value);
                var docHash = doc[primaryKey].ToString().ToHash();
                IDictionary<string, object> existingDoc;

                if (documentsByPrimaryKey.TryGetValue(docHash, out existingDoc))
                {
                    foreach (var field in doc)
                    {
                        // TODO: add condition for when there are two doc versions with different created dates.
                        if (field.Key != primaryKey && select.Contains(field.Key))
                        {
                            object existingValue;

                            if (existingDoc.TryGetValue(field.Key, out existingValue))
                            {
                                existingDoc[field.Key] = new object[] { existingValue, field.Value };
                            }
                            else
                            {
                                existingDoc[field.Key] = field.Value;
                            }
                        }
                    }

                    existingDoc[SystemFields.Score] = (double)existingDoc[SystemFields.Score] + (double)doc[SystemFields.Score]; 
                }
                else
                {
                    doc[SystemFields.Score] = (double)doc[SystemFields.Score];
                    result.Add(doc);
                    documentsByPrimaryKey.Add(docHash, doc);
                }
            }

            _logger.LogInformation($"reading documents took {timer.Elapsed}");
            timer.Restart();

            result.Sort(
                delegate (IDictionary<string, object> doc1,
                IDictionary<string, object> doc2)
                {
                    return ((double)doc2[SystemFields.Score]).CompareTo((double)doc1[SystemFields.Score]);
                });

            _logger.LogInformation($"second sorting took {timer.Elapsed}");

            return result;
        }

        public IColumnReader CreateColumnReader(ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            return new ColumnStreamReader(
                    collectionId,
                    keyId,
                    _sessionFactory,
                    _logger);
        }

        public override void Dispose()
        {
            base.Dispose();

            _postingsReader.Dispose();
        }
    }
}

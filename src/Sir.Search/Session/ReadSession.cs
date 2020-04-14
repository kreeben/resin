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
    public class ReadSession : DocumentStreamSession, IDisposable, IReadSession
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

        public ReadResult Read(IQuery query, int skip, int take)
        {
            var result = MapReduceSort(query, skip, take);

            if (result != null)
            {
                var scoreDivider = query.GetDivider();
                var docs = ReadDocs(result.SortedDocuments, scoreDivider, query.Select);

                return new ReadResult { Query = query, Total = result.Total, Docs = docs };
            }

            return new ReadResult { Query = query, Total = 0, Docs = new IDictionary<string, object>[0] };
        }

        private ScoredResult MapReduceSort(IQuery query, int skip, int take)
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
            int scoreDivider,
            HashSet<string> select)
        {
            var result = new List<IDictionary<string, object>>();
            var timer = Stopwatch.StartNew();

            foreach (var d in docIds)
            {
                var doc = ReadDoc(d.Key, select, (d.Value/scoreDivider)*100);

                result.Add(doc);
            }

            _logger.LogInformation($"reading documents took {timer.Elapsed}");

            return result;
        }

        public IColumnReader CreateColumnReader(ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(_sessionFactory.Dir, string.Format("{0}.{1}.ix", collectionId, keyId));

            if (!File.Exists(ixFileName))
                return null;

            return new ColumnReader(
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using log4net;
using Resin.Analysis;
using Resin.IO;
using Resin.IO.Read;
using Resin.Querying;
using Resin.Sys;

namespace Resin
{
    /// <summary>
    /// Query indices in a directory.
    /// </summary>
    public class Searcher : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Searcher));
        private readonly string _directory;
        private readonly QueryParser _parser;
        private readonly IScoringScheme _scorerFactory;
        private readonly IList<IxInfo> _ixs;
        private readonly int _blockSize;
        private readonly int _documentCount;
        private IDistanceResolver _distanceResolver;

        public Searcher(string directory)
            :this(directory, new QueryParser(new Analyzer()), new Tfidf(), new Levenshtein())
        {
        }

        public Searcher(string directory, QueryParser parser, IScoringScheme scorerFactory, IDistanceResolver distanceResolver)
        {
            _directory = directory;
            _parser = parser;
            _scorerFactory = scorerFactory;

            _ixs = Util.GetIndexFileNamesInChronologicalOrder(directory).Select(IxInfo.Load).ToList();

            _documentCount = Util.GetDocumentCount(_ixs);

            _blockSize = Serializer.SizeOfBlock();

            _distanceResolver = distanceResolver;
        }

        public Result Search(string query, int page = 0, int size = 10000)
        {
            var searchTime = new Stopwatch();
            searchTime.Start();

            var queryContext = _parser.Parse(query);

            if (queryContext == null)
            {
                return new Result { Docs = new List<ScoredDocument>() };
            }

            var skip = page * size;
            var scored = Collect(queryContext);
            var paged = scored.OrderByDescending(s=>s.Score).Skip(skip).Take(size).ToList();
            var docs = new List<ScoredDocument>();
            var result = new Result { Total = scored.Count};
            var groupedByIx = paged.GroupBy(s => s.Ix);

            var docTime = new Stopwatch();
            docTime.Start(); 
            
            foreach (var group in groupedByIx)
            {
                docs.AddRange(GetDocs(group.ToList(), group.Key));
            }

            result.Docs = docs.OrderByDescending(d => d.Score).ToList();

            Log.DebugFormat("fetched {0} docs for query {1} in {2}", docs.Count, queryContext, docTime.Elapsed);
            Log.DebugFormat("searched {0} in {1}", queryContext, searchTime.Elapsed);

            return result;
        }

        private IList<DocumentScore> Collect(QueryContext query)
        {
            var results = new List<IList<DocumentScore>>();

            foreach (var ix in _ixs)
            {
                using (var collector = new Collector(_directory, ix, _scorerFactory, _distanceResolver, _documentCount))
                {
                    results.Add(collector.Collect(query).ToList());
                }
            }

            var timer = new Stopwatch();
            timer.Start();

            if (results.Count == 1)
            {
                return results[0];
            }
            
            var agg = results.CombineTakingLatestVersion().ToList();

            Log.DebugFormat("reduced multi-index collections for query {0} in {1}", query, timer.Elapsed);

            return agg;
        }

        private IEnumerable<ScoredDocument> GetDocs(IList<DocumentScore> scores, IxInfo ix)
        {
            var docAddressFileName = Path.Combine(_directory, ix.VersionId + ".da");

            IList<BlockInfo> docAdrs;

            using (var docAddressReader = new DocumentAddressReader(
                new FileStream(docAddressFileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096 * 1, FileOptions.SequentialScan)))
            {
                var adrs = scores
                    .Select(s => new BlockInfo(s.DocumentId * _blockSize, _blockSize))
                    .OrderBy(b => b.Position)
                    .ToList();

                docAdrs = docAddressReader.Get(adrs).ToList();
            }

            var docFileName = Path.Combine(_directory, ix.VersionId + ".rdoc");

            using (var docReader = new DocumentReader(
                new FileStream(docFileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096*4, FileOptions.SequentialScan),
                (Compression)ix.Compression))
            {
                var dic = scores.ToDictionary(x => x.DocumentId, y => y.Score);

                foreach (var doc in docReader.Get(docAdrs))
                {
                    var score = dic[doc.Id];

                    yield return new ScoredDocument{Document = doc, Score = score};
                }
            }
        }

        public void Dispose()
        {
        }
    }
}
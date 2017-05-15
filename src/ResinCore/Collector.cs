using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using log4net;
using Resin.Analysis;
using Resin.IO;
using Resin.IO.Read;
using Resin.Querying;
using Resin.Sys;

namespace Resin
{
    public class Collector : IDisposable
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(Collector));
        private readonly string _directory;
        private readonly IxInfo _ix;
        private readonly IScoringScheme _scorerFactory;
        private readonly int _documentCount;
        private readonly IDistanceResolver _distanceResolver;
        private readonly IDictionary<SubQuery, IList<DocumentScore>> _scoreCache;

        public IxInfo Ix { get { return _ix; } }

        public Collector(string directory, IxInfo ix, IScoringScheme scorerFactory = null, IDistanceResolver distanceResolver = null, int documentCount = -1)
        {
            _directory = directory;
            _ix = ix;
            _scorerFactory = scorerFactory;
            _distanceResolver = distanceResolver ?? new Levenshtein();
            _documentCount = documentCount == -1 ? ix.DocumentCount : documentCount;
            _scoreCache = new Dictionary<SubQuery, IList<DocumentScore>>();
        }

        public IList<DocumentScore> Collect(QueryContext query)
        {
            var scoreTime = new Stopwatch();
            scoreTime.Start();

            foreach (var subQuery in query.ToList())
            {
                IList<DocumentScore> scores;

                if (!_scoreCache.TryGetValue(subQuery, out scores))
                {
                    GetTerms(subQuery);
                    Score(subQuery);

                    _scoreCache.Add(subQuery, subQuery.Scored.ToList());
                }
                else
                {
                    subQuery.Scored = _scoreCache[subQuery];
                }
            }

            var reduceTime = new Stopwatch();
            reduceTime.Start();

            var reduced = query.Reduce().ToList();

            Log.DebugFormat("reduced query {0} producing {1} scores in {2}", query, reduced.Count, scoreTime.Elapsed);

            return reduced;
        }

        private void Scan(IEnumerable<QueryContext> queries)
        {
            //Parallel.ForEach(queries, GetTerms);

            foreach (var q in queries)
            {
                GetTerms(q);
            }
        }

        private void GetTerms(QueryContext query)
        {
            var time = new Stopwatch();
            time.Start();

            var reader = GetTreeReader(query.Field);

            if (reader == null)
            {
                query.Terms = Enumerable.Empty<Term>();
            }
            else
            {
                if (query.Fuzzy)
                {
                    query.Terms = reader.Near(query.Value, query.Edits, _distanceResolver)
                        .Select(word => new Term(query.Field, word))
                        .ToList();
                }
                else if (query.Prefix)
                {
                    query.Terms = reader.StartsWith(query.Value).Select(word => new Term(query.Field, word)).ToList();
                }
                else
                {
                    var terms = new List<Term>();
                    Word word;
                    if (reader.HasWord(query.Value, out word))
                    {
                        terms.Add(new Term(query.Field, word));
                    }
                    query.Terms = terms;
                }
            }

            Log.DebugFormat("scanned {0} in {1}", query.AsReadable(), time.Elapsed);

            GetPostings(query);
        }

        private void GetPostings(QueryContext query)
        {
            var time = new Stopwatch();
            time.Start();

            var terms = query.Terms.ToList();
            var postings = terms.Count > 0 ? ReadPostings(terms).ToList() : null;

            IEnumerable<DocumentPosting> result;

            if (postings == null)
            {
                result = null;
                
            }
            else
            {
                result = postings.Sum();
            }

            query.Postings = result;

            Log.DebugFormat("read postings for {0} in {1}", query.AsReadable(), time.Elapsed);
        }
        
        private IEnumerable<IList<DocumentPosting>> ReadPostings(IEnumerable<Term> terms)
        {
            return PostingsReader.ReadPostings(_directory, _ix, terms);
        }

        private void Score(IEnumerable<QueryContext> queries)
        {
            Parallel.ForEach(queries, Score);
        }

        private void Score(QueryContext query)
        {
            if (query.Postings == null)
            {
                query.Scored = new List<DocumentScore>();
            }
            else
            {
                query.Scored = DoScore(query.Postings.OrderBy(p => p.DocumentId).ToList());
            }
        }

        private IEnumerable<DocumentScore> DoScore(IList<DocumentPosting> postings)
        {
            var docHashesFileName = Path.Combine(_directory, string.Format("{0}.{1}", _ix.VersionId, "pk"));

            using (var docHashReader = new DocHashReader(docHashesFileName))
            {
                if (_scorerFactory == null)
                {
                    foreach (var posting in postings)
                    {
                        var docHash = docHashReader.Read(posting.DocumentId);

                        if (!docHash.IsObsolete)
                        {
                            yield return new DocumentScore(posting.DocumentId, docHash.Hash, 0, _ix);
                        }
                    }
                }
                else
                {
                    if (postings.Any())
                    {
                        var docsWithTerm = postings.Count;

                        var scorer = _scorerFactory.CreateScorer(_documentCount, docsWithTerm);

                        foreach (var posting in postings)
                        {
                            var docHash = docHashReader.Read(posting.DocumentId);

                            if (!docHash.IsObsolete)
                            {
                                var score = scorer.Score(posting);

                                yield return new DocumentScore(posting.DocumentId, docHash.Hash, score, _ix);
                            }
                        }
                    }
                }
            }
        }

        private ITrieReader GetTreeReader(string field)
        {
            var fileName = Path.Combine(_directory, string.Format("{0}-{1}.tri", 
                _ix.VersionId, field.ToHash()));

            if (!File.Exists(fileName)) return null;

            return new MappedTrieReader(fileName);
        }

        public void Dispose()
        {
        }
    }
}
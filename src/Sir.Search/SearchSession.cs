﻿using Microsoft.Extensions.Logging;
using Sir.IO;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;

namespace Sir
{
    /// <summary>
    /// Read session targeting multiple collections.
    /// </summary>
    public class SearchSession : DocumentStreamSession, IDisposable, ISearchSession
    {
        private readonly IStreamDispatcher _sessionFactory;
        private readonly IModel _model;
        private readonly IIndexReadWriteStrategy _indexStrategy;
        private readonly PostingsResolver _postingsResolver;
        private readonly Scorer _scorer;
        private readonly ILogger _logger;
        private readonly Dictionary<(string, ulong, long), ColumnReader> _readers;

        public SearchSession(
            string directory,
            IStreamDispatcher sessionFactory,
            IModel model,
            IIndexReadWriteStrategy indexStrategy,
            ILogger logger = null,
            PostingsResolver postingsResolver = null,
            Scorer scorer = null) : base(directory, sessionFactory)
        {
            _sessionFactory = sessionFactory;
            _model = model;
            _indexStrategy = indexStrategy;
            _postingsResolver = postingsResolver ?? new PostingsResolver();
            _scorer = scorer ?? new Scorer();
            _logger = logger;
            _readers = new Dictionary<(string, ulong, long), ColumnReader>();
        }

        public SearchResult Search(IQuery query, int skip, int take)
        {
            var result = Execute(query, skip, take, false);

            if (result != null)
            {
                var numOfTerms = query.TotalNumberOfTerms();
                var scoreMultiplier = (double)1 / numOfTerms;
                var docs = ReadDocs(result.SortedDocuments, query.Select, scoreMultiplier);

                return new SearchResult(query, result.Total, docs.Count, docs);
            }

            return new SearchResult(query, 0, 0, System.Linq.Enumerable.Empty<Document>());
        }

        public Document SearchScalar(IQuery query)
        {
            var result = Execute(query, 0, 1, true);

            if (result != null)
            {
                var numOfTerms = query.TotalNumberOfTerms();
                var scoreMultiplier = (double)1 / numOfTerms;
                var docs = ReadDocs(result.SortedDocuments, query.Select, scoreMultiplier);

                return docs.Count > 0 ? docs[0] : null;
            }

            return null;
        }

        public SearchResult SearchIdentical(IQuery query, int take)
        {
            var result = Execute(query, 0, take, true);

            if (result != null)
            {
                var numOfTerms = query.TotalNumberOfTerms();
                var scoreMultiplier = (double)1 / numOfTerms;
                var docs = ReadDocs(result.SortedDocuments, query.Select, scoreMultiplier);

                return new SearchResult(query, result.Total, docs.Count, docs);
            }

            return new SearchResult(query, 0, 0, System.Linq.Enumerable.Empty<Document>());
        }

        private ScoredResult Execute(IQuery query, int skip, int take, bool identicalMatchesOnly)
        {
            var timer = Stopwatch.StartNew();

            // Scan index
            Scan(query, identicalMatchesOnly);
            LogDebug($"scanning took {timer.Elapsed}");
            timer.Restart();

            // Read postings lists
            _postingsResolver.Resolve(query, _sessionFactory, _logger);
            LogDebug($"reading postings took {timer.Elapsed}");
            timer.Restart();
            
            // Score
            IDictionary<(ulong CollectionId, long DocumentId), double> scoredResult = new Dictionary<(ulong, long), double>();
            _scorer.Score(query, ref scoredResult);
            LogDebug($"scoring took {timer.Elapsed}");
            timer.Restart();

            // Sort
            var sorted = Sort(scoredResult, skip, take);
            LogDebug($"sorting took {timer.Elapsed}");

            return sorted;
        }

        /// <summary>
        /// Scans the index to find the query's closest matching nodes and records their posting list addresses.
        /// </summary>
        private void Scan(IQuery query, bool identicalMatchesOnly)
        {
            if (query == null)
                return;

            foreach (var term in query.AllTerms())
            {
                ColumnReader reader;
                var key = (term.Directory, term.CollectionId, term.KeyId);

                if (!_readers.TryGetValue(key, out reader))
                {
                    reader = CreateColumnReader(term.Directory, term.CollectionId, term.KeyId);

                    if (reader != null)
                    {
                        _readers.Add(key, reader);
                    }
                }

                if (reader != null)
                {
                    var hit = _indexStrategy.GetMatchOrNull(term.Vector, _model, reader);

                    if (hit != null)
                    {
                        if ((identicalMatchesOnly && hit.Score >= _model.IdenticalAngle) || !identicalMatchesOnly)
                        {
                            term.Score = hit.Score;
                            term.PostingsOffsets = hit.PostingsOffsets;
                        }
                    }
                }
            }
        }

        private ColumnReader CreateColumnReader(string directory, ulong collectionId, long keyId)
        {
            var ixFileName = Path.Combine(directory, string.Format("{0}.{1}.ix", collectionId, keyId));
            var vectorFileName = Path.Combine(directory, $"{collectionId}.{keyId}.vec");
            var pageIndexFileName = Path.Combine(directory, $"{collectionId}.{keyId}.ixtp");

            using (var pageIndexReader = new PageIndexReader(_sessionFactory.CreateReadStream(pageIndexFileName)))
            {
                return new ColumnReader(
                    pageIndexReader.ReadAll(),
                    _sessionFactory.CreateReadStream(ixFileName),
                    _sessionFactory.CreateReadStream(vectorFileName));
            }
        }

        private static ScoredResult Sort(
            IDictionary<(ulong CollectionId, long DocumentId), double> documents,
            int skip, 
            int take)
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

        private IList<Document> ReadDocs(
            IEnumerable<KeyValuePair<(ulong collectionId, long docId), double>> docIds, 
            HashSet<string> select,
            double scoreMultiplier = 1)
        {
            var result = new List<Document>();
            var timer = Stopwatch.StartNew();

            foreach (var d in docIds)
            {
                var doc = ReadDocument(d.Key, select, d.Value * scoreMultiplier);

                if (doc != null)
                    result.Add(doc);
            }

            LogDebug($"reading documents took {timer.Elapsed}");

            return result;
        }

        private void LogInformation(string message)
        {
            if (_logger != null)
                _logger.LogInformation(message);
        }

        private void LogDebug(string message)
        {
            if (_logger != null)
                _logger.LogDebug(message);
        }

        private void LogError(Exception ex, string message)
        {
            if (_logger != null)
                _logger.LogError(ex, message);
        }

        public override void Dispose()
        {
            if (_postingsResolver!= null)
                _postingsResolver.Dispose();

            foreach (var reader in _readers.Values)
            {
                reader.Dispose();
            }

            base.Dispose();
        }
    }
}
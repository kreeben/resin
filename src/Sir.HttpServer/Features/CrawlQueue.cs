using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Sir.Core;
using Sir.Search;

namespace Sir.HttpServer.Features
{
    public class CrawlQueue : IDisposable
    {
        private readonly ProducerConsumerQueue<CrawlJob> _queue;
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser _queryParser;
        private readonly IStringModel _model;
        private readonly ILogger _logger;
        private readonly IReadSession _readSession;
        private readonly HashSet<string> _enquedIds;
        private readonly HashSet<string> _wetStoredFieldNames;
        private readonly HashSet<string> _wetIndexedFieldNames;
        private readonly HashSet<string> _mapIndexedFieldNames;

        public (Uri uri, string title) LastProcessed { get; private set; }

        public CrawlQueue(
            SessionFactory sessionFactory, 
            QueryParser queryParser,
            IStringModel model, 
            ILogger<CrawlQueue> logger)
        {
            _queue = new ProducerConsumerQueue<CrawlJob>(1, DispatchJob);
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _model = model;
            _logger = logger;
            _readSession = sessionFactory.CreateReadSession();
            _enquedIds = new HashSet<string>();

            _wetStoredFieldNames = new HashSet<string> { "url", "description" };
            _wetIndexedFieldNames = new HashSet<string> { "url", "warcid" };
            _mapIndexedFieldNames = new HashSet<string> { "title","description", "description", "scheme", "host", "path", "query", "url", "filename" };
        }

        public void Enqueue(CrawlJob job)
        {
            if (_enquedIds.Add(job.Id))
            {
                _queue.Enqueue(job);
            }
        }

        private void DispatchJob(CrawlJob crawlJob)
        {
            try
            {
                if (crawlJob.Job == "map")
                {
                    Map(crawlJob);
                }

                _enquedIds.Remove(crawlJob.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {crawlJob} {ex}");
            }
        }

        private void Map(CrawlJob crawlJob)
        {
            var orignalQuery = _queryParser.Parse(crawlJob.Collection, crawlJob.Q, crawlJob.Field, and: crawlJob.And, or: crawlJob.Or);
            
            var originalResult = _readSession.Read(orignalQuery, 0, int.MaxValue).Docs
                .ToDictionary(x=>(long)x["___docid"]);

            var wetFiles = new SortedList<string, object>();
            ReadResult wetRecords = null;
            var wetCollectionId = "cc_wet".ToHash();

            foreach (var doc in originalResult.Values)
            {
                var wetFileName = ((string)doc["filename"]).Replace("/warc", "/wet").Replace(".gz", ".wet.gz");

                wetFiles.TryAdd(wetFileName, null);
            }

            using (var client = new WebClient())
            {
                foreach (var warcId in wetFiles.Keys)
                {
                    var wetQuery = _queryParser.Parse(
                        new string[] { "cc_wet" },
                        warcId,
                        new string[] { "warcid" }, and: true, or: false);

                    if (wetQuery != null)
                    {
                        wetRecords = _readSession.Read(wetQuery, 0, 1);
                    }

                    if (wetRecords == null || wetRecords.Total == 0)
                    {
                        var localFileName = Path.Combine(_sessionFactory.Dir, "wet", warcId);

                        if (!File.Exists(localFileName))
                        {
                            if (!Directory.Exists(Path.GetDirectoryName(localFileName)))
                            {
                                Directory.CreateDirectory(Path.GetDirectoryName(localFileName));
                            }

                            var remoteFileName = $"https://commoncrawl.s3.amazonaws.com/{warcId}";

                            client.DownloadFile(remoteFileName, localFileName);
                        }

                        var time = Stopwatch.StartNew();

                        var writeJob = new Job(
                            wetCollectionId,
                            ReadWetFile(localFileName, warcId),
                            _model,
                            _wetStoredFieldNames,
                            _wetIndexedFieldNames);

                        _sessionFactory.Write(writeJob, reportSize: 1000);

                        _logger.LogInformation($"wet file write job took {time.Elapsed}");

                        wetQuery = _queryParser.Parse(
                            new string[] { "cc_wet" },
                            warcId,
                            new string[] { "warcid" }, and: true, or: false);

                        wetRecords = _readSession.Read(wetQuery, 0, 1);
                    }
                }
            }

            if (wetRecords == null || wetRecords.Total == 0)
            {
                throw new DataMisalignedException();
            }
        }

        public static IEnumerable<IDictionary<string,object>> ReadWetFile(string fileName, string warcId)
        {
            const string uriLabel = "WARC-Target-URI: ";
            const string contentLabel = "Content-Length: ";
            const string contentEndLabel = "WARC/1.0";

            string url = null;
            var content = new StringBuilder();
            bool isContent = false;

            var lines = ReadAllLinesFromGz(fileName).Skip(15);

            foreach (var line in lines)
            {
                if (isContent)
                {
                    if (line.Length > 0)
                        content.AppendLine(line);
                }

                if (line.StartsWith(contentEndLabel))
                {
                    isContent = false;

                    if (content.Length > 0)
                    {
                        yield return new Dictionary<string, object>
                    {
                        { "url", url},
                        { "description", content.ToString() },
                        { "warcid", warcId }
                    };

                        content = new StringBuilder();
                    }
                }
                else if (line.StartsWith(uriLabel))
                {
                    url = line.Replace(uriLabel, "");
                }
                else if (line.StartsWith(contentLabel))
                {
                    isContent = true;
                }
            }
        }

        private static IEnumerable<string> ReadAllLinesFromGz(string fileName)
        {
            using (var stream = File.OpenRead(fileName))
            using (var zip = new GZipStream(stream, CompressionMode.Decompress))
            using (var reader = new StreamReader(zip))
            {
                var line = reader.ReadLine();

                while (line != null)
                {
                    yield return line;

                    line = reader.ReadLine();
                }
            }
        }

        public void Dispose()
        {
            _queue.Dispose();
            _readSession.Dispose();
        }
    }

    public class CrawlJob : IEquatable<CrawlJob>
    {
        public string Id { get; }
        public string[] Collection { get; }
        public string[] Field { get; }
        public string Q { get; }
        public string Target { get; }
        public string Job { get; }
        public bool And { get; }
        public bool Or { get; }

        public CrawlJob(string id, string[] collection, string[] field, string q, string target, string job, bool and, bool or)
        {
            Id = id;
            Collection = collection;
            Field = field;
            Q = q;
            Target = target;
            Job = job;
            And = and;
            Or = or;
        }

        public override bool Equals(object obj)
        {
            return obj is CrawlJob job &&
                   EqualityComparer<string[]>.Default.Equals(Collection, job.Collection) &&
                   EqualityComparer<string[]>.Default.Equals(Field, job.Field) &&
                   Q == job.Q &&
                   Target == job.Target &&
                   Job == job.Job;
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Collection, Field, Q, Target, Job);
        }

        public bool Equals([AllowNull] CrawlJob other)
        {
            if (other == null)
                return false;

            return 
                EqualityComparer<string[]>.Default.Equals(Collection, other.Collection) &&
                EqualityComparer<string[]>.Default.Equals(Field, other.Field) &&
                Q == other.Q &&
                Target == other.Target &&
                Job == other.Job;
        }

        public static bool operator ==(CrawlJob left, CrawlJob right)
        {
            return EqualityComparer<CrawlJob>.Default.Equals(left, right);
        }

        public static bool operator !=(CrawlJob left, CrawlJob right)
        {
            return !(left == right);
        }

        public override string ToString()
        {
            return $"{Collection} {Field} {Q} {Target} {Job}";
        }
    }
}
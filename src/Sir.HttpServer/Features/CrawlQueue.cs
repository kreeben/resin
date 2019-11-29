using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using HtmlAgilityPack;
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

        public (Uri uri, string title) LastProcessed { get; private set; }

        public CrawlQueue(
            SessionFactory sessionFactory, 
            QueryParser queryParser,
            IStringModel model, 
            ILogger<CrawlQueue> logger)
        {
            _queue = new ProducerConsumerQueue<CrawlJob>(1, DoWork);
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _model = model;
            _logger = logger;
            _readSession = sessionFactory.CreateReadSession();
            _enquedIds = new HashSet<string>();
        }

        public void Enqueue(CrawlJob job)
        {
            if (_enquedIds.Add(job.Id))
            {
                _queue.Enqueue(job);
            }
        }

        private void DoWork(CrawlJob crawlJob)
        {
            try
            {
                var query = _queryParser.Parse(crawlJob.Collection, crawlJob.Q, crawlJob.Field, and: true, or: false);
                var readResult = _readSession.Read(query, 0, int.MaxValue);
                var wetFiles = new SortedList<string, object>();

                foreach (var doc in readResult.Docs)
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
                            new string[] { "filename" }, and: true, or: false);

                        ReadResult existingWetRecords = null;

                        if (wetQuery != null)
                        {
                            existingWetRecords = _readSession.Read(wetQuery, 0, 1);
                        }

                        if (existingWetRecords == null || existingWetRecords.Total == 0)
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
                            var collectionId = "cc_wet".ToHash();
                            var writeJob = new Job(collectionId, ReadWetFile(localFileName), _model);

                            _sessionFactory.WriteConcurrent(writeJob, reportSize:1000);

                            _logger.LogInformation($"write job took {time.Elapsed}");

                            existingWetRecords = _readSession.Read(wetQuery, 0, int.MaxValue);
                        }

                        if (existingWetRecords.Total == 0)
                        {
                            throw new DataMisalignedException();
                        }

                        using (var writeSession = _sessionFactory.CreateWriteSession(crawlJob.Target.ToHash()))
                        {
                            foreach (var doc in existingWetRecords.Docs)
                                writeSession.Write(doc);
                        }
                    }
                }

                _enquedIds.Remove(crawlJob.Id);
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {crawlJob} {ex}");
            }
        }

        public static IEnumerable<IDictionary<string,object>> ReadWetFile(string fileName)
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
                        { "description", content.ToString() }
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

        public string GetTitle(Uri uri)
        {
            try
            {
                var url = uri.ToString().Replace(uri.Scheme + "://", string.Empty);
                var str = GetWebString(uri);

                if (str == null)
                {
                    return string.Empty;
                }

                var html = new HtmlDocument();

                html.LoadHtml(str);

                var doc = Parse(html, uri);

                if (doc.title == null)
                {
                    return string.Empty;
                }

                return doc.title;
            }
            catch
            {
                return string.Empty;
            }
        }

        private IDictionary<string, object> GetDocument(string collectionName, string url, string title)
        {
            const string key = "_url";

            var collectionId = collectionName.ToHash();

            var keyId = _sessionFactory.GetKeyId(collectionName.ToHash(), key.ToHash());

            var urlQuery = new Query(
                _model.Tokenize(url)
                    .Select(x => new Term(collectionId, keyId, key, x, and: true, or: false, not: false)).ToList(),
                and: true,
                or: false,
                not: false
            );

            using (var readSession = _sessionFactory.CreateReadSession())
            {
                var result = readSession.Read(urlQuery, 0, 1).Docs.ToList();

                return result.Count == 0
                    ? null : (float)result[0]["___score"] >= _model.IdenticalAngle
                    ? result[0] : null;
            }
        }

        public void ExecuteWrite(string collectionName, IDictionary<string, object> doc)
        {
            _sessionFactory.WriteConcurrent(new Job(collectionName.ToHash(), new[] { doc }, _model));
        }

        private string GetWebString(Uri uri)
        {
            var urlStr = uri.ToString();

            try
            {
                var req = (HttpWebRequest)WebRequest.Create(uri);
                req.ReadWriteTimeout = 10 * 1000;
                req.Headers.Add("User-Agent", "dygg-robot/didyougogo.com");
                req.Headers.Add("Accept", "text/html");

                using (var response = (HttpWebResponse)req.GetResponse())
                using (var content = response.GetResponseStream())
                using (var reader = new StreamReader(content))
                {
                    if (!response.GetResponseHeader("Content-Type").Contains("text/html"))
                    {
                        return null;
                    }

                    if (response.StatusCode != HttpStatusCode.OK)
                    {
                        _logger.LogInformation(string.Format("bad request: {0} response: {1}", uri, response.StatusCode));

                        return null;
                    }

                    _logger.LogInformation(string.Format("requested: {0}", uri));

                    return reader.ReadToEnd();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(string.Format("request failed: {0} {1}", uri, ex));

                return null;
            }
        }

        private static HashSet<string> GetForbiddenUrls(string robotTxt)
        {
            var result = new HashSet<string>();

            foreach (var line in robotTxt.Split('\r', '\n'))
            {
                var parts = line.ToLower().Split(':');

                if (parts[0].Trim() == "disallow")
                {
                    var rule = parts[1].Trim(' ', '?').Replace("/*", string.Empty);

                    if (rule != "/")
                        result.Add(rule);
                }
            }

            return result;
        }

        private (string title, string body) Parse(HtmlDocument htmlDocument, Uri owner)
        {
            var titleNodes = htmlDocument.DocumentNode.SelectNodes("//title");

            if (titleNodes == null) return (null, null);

            var titleNode = titleNodes.FirstOrDefault();

            if (titleNode == null) return (null, null);

            var title = WebUtility.HtmlDecode(titleNode.InnerText);
            var root = htmlDocument.DocumentNode.SelectNodes("//body").First();
            var txtNodes = root.Descendants().Where(x =>
                x.Name == "#text" &&
                (x.ParentNode.Name != "script") &&
                (!string.IsNullOrWhiteSpace(x.InnerText))
            ).ToList();

            var ownerUrl = owner.Host;
            var txt = txtNodes.Select(x => WebUtility.HtmlDecode(x.InnerText));
            var body = string.Join("\r\n", txt);

            return (title, body);
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

        public CrawlJob(string id, string[] collection, string[] field, string q, string target, string job)
        {
            Id = id;
            Collection = collection;
            Field = field;
            Q = q;
            Target = target;
            Job = job;
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
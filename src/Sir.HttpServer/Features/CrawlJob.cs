using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.Extensions.Logging;
using Sir.Search;

namespace Sir.HttpServer.Features
{
    public class CrawlJob : AsyncJob
    {
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser _queryParser;
        private readonly IReadSession _readSession;
        private readonly ILogger<CrawlJob> _logger;
        private readonly IStringModel _model;
        private readonly HashSet<string> _wetStoredFieldNames;
        private readonly HashSet<string> _wetIndexedFieldNames;

        public CrawlJob(
            SessionFactory sessionFactory,
            QueryParser queryParser,
            IStringModel model,
            ILogger<CrawlJob> logger,
            string id, 
            string[] collection, 
            string[] field, 
            string q, 
            string job, 
            bool and, 
            bool or) 
            : base(id, collection, field, q, job, and, or)
        {
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _wetStoredFieldNames = new HashSet<string> { "url", "description" };
            _wetIndexedFieldNames = new HashSet<string> { "url", "warcid" };
        }

        public override void Execute()
        {
            try
            {
                if (Job == "ccc")
                {
                    FetchWetFile();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {this} {ex}");
            }
        }

        private void FetchWetFile()
        {
            var orignalQuery = _queryParser.Parse(Collection, Q, Field, and: And, or: Or);

            using (var readSession = _sessionFactory.CreateReadSession())
            {
                var originalResult = readSession.Read(orignalQuery, 0, int.MaxValue).Docs
                    .ToDictionary(x => (long)x["___docid"]);

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
                            wetRecords = readSession.Read(wetQuery, 0, 1);
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

                            wetRecords = readSession.Read(wetQuery, 0, 1);
                        }
                    }
                }

                if (wetRecords == null || wetRecords.Total == 0)
                {
                    throw new DataMisalignedException();
                }
            }
        }

        private static IEnumerable<IDictionary<string, object>> ReadWetFile(string fileName, string warcId)
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
    }
}
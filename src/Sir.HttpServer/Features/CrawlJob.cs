using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using Microsoft.Extensions.Logging;
using Sir.Search;

namespace Sir.HttpServer.Features
{
    public class CrawlJob : AsyncJob
    {
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser _queryParser;
        private readonly ILogger<CrawlJob> _logger;
        private readonly IStringModel _model;
        private readonly HashSet<string> _wetStoredFieldNames;
        private readonly HashSet<string> _wetIndexedFieldNames;
        private readonly int _skip;
        private readonly int _take;

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
            bool or,
            int skip,
            int take) 
            : base(id, collection, field, q, job, and, or)
        {
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _wetStoredFieldNames = new HashSet<string> { "url", "title", "description", "filename" };
            _wetIndexedFieldNames = new HashSet<string> { "title", "description" };
            _skip = skip;
            _take = take;

            Status["download"] = 0;
            Status["index"] = 0;
        }

        public override void Execute()
        {
            try
            {
                if (Job == "CCC")
                {
                    DownloadAndIndexWetFile();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError($"error processing {this} {ex}");
            }
        }

        private void DownloadAndIndexWetFile()
        {
            var writePayload = new List<IDictionary<string, object>>();

            var originalQuery = _queryParser.Parse(
                Collections, 
                Q, 
                Fields, 
                select: new string[] {"url", "title", "filename"},
                and: And, 
                or: Or);

            using (var readSession = _sessionFactory.CreateReadSession())
            {
                var originalResult = readSession.Read(originalQuery, _skip, _take)
                    .Docs
                    .ToDictionary(x => (string)x["url"]);

                var wetFileIds = new SortedList<string, object>();
                ReadResult wetResult = null;
                var wetCollectionId = "cc_wet".ToHash();

                foreach (var doc in originalResult.Values)
                {
                    var wetFileId = ((string)doc["filename"]).Replace("/warc", "/wet").Replace(".gz", ".wet.gz");

                    wetFileIds.TryAdd(wetFileId, null);

                    //TODO: remove break
                    break;
                }

                foreach (var fileName in wetFileIds.Keys)
                {
                    var wetQuery = _queryParser.Parse(
                        collections: new string[] { "cc_wet" },
                        q: fileName,
                        fields: new string[] { "filename" },
                        select: new string[] { "filename" },
                        and: true,
                        or: false);

                    if (wetQuery != null)
                    {
                        wetResult = readSession.Read(wetQuery, 0, 1);
                    }

                    if (wetResult == null || wetResult.Total == 0)
                    {
                        var localFileName = Path.Combine(_sessionFactory.Dir, "wet", fileName);
                        var tmpFileName = Path.Combine(_sessionFactory.Dir, "tmp", Id, fileName);

                        if (!File.Exists(localFileName))
                        {
                            if (!Directory.Exists(Path.GetDirectoryName(tmpFileName)))
                            {
                                Directory.CreateDirectory(Path.GetDirectoryName(tmpFileName));
                            }

                            var remoteFileName = $"https://commoncrawl.s3.amazonaws.com/{fileName}";
                            const double payloadSize = 150000000;

                            using (var client = new WebClient())
                            {
                                var state = new State { Completed = false };
                                client.DownloadFileCompleted += Client_DownloadFileCompleted;
                                client.DownloadFileAsync(new Uri(remoteFileName), tmpFileName, state);

                                while (!state.Completed)
                                {
                                    try
                                    {
                                        if (File.Exists(tmpFileName))
                                        {
                                            var fi = new FileInfo(tmpFileName);

                                            if (fi.Length > 0)
                                            {
                                                var status = (fi.Length / (payloadSize * wetFileIds.Count)) * 100;

                                                Status["download"] = status;
                                            }
                                        }
                                    }
                                    catch { }
                                    finally
                                    {
                                        Thread.Sleep(1000);
                                    }
                                }
                            }
                        }

                        if (!File.Exists(localFileName))
                        {
                            try
                            {
                                var localDir = Path.GetDirectoryName(localFileName);

                                if (!Directory.Exists(localDir))
                                {
                                    Directory.CreateDirectory(localDir);
                                }

                                File.Move(tmpFileName, localFileName, true);
                                Thread.Sleep(100);
                                Directory.Delete(Path.GetDirectoryName(tmpFileName));
                            }
                            catch (Exception ex)
                            {
                                _logger.LogError(ex, ex.Message);
                            }
                        }

                        foreach (var document in ReadWetFile(localFileName, fileName))
                        {
                            IDictionary<string, object> originalDoc;
                            var key = (string)document["url"];

                            if (originalResult.TryGetValue(key, out originalDoc))
                            {
                                document["title"] = originalDoc["title"];
                                document["filename"] = originalDoc["filename"];

                                writePayload.Add(document);
                            }
                        }
                    }
                }

                Status["download"] = 100;

                if (writePayload.Count > 0)
                {
                    var time = Stopwatch.StartNew();

                    var writeJob = new WriteJob(
                        wetCollectionId,
                        writePayload,
                        new BocModel(),
                        _wetStoredFieldNames,
                        _wetIndexedFieldNames);

                    _sessionFactory.Write(writeJob, reportSize: 1000);

                    Status["index"] = 100;

                    _logger.LogInformation($"wet file write job took {time.Elapsed}");
                }
            }
        }

        private void Client_DownloadFileCompleted(object sender, System.ComponentModel.AsyncCompletedEventArgs e)
        {
            ((State)e.UserState).Completed = true;
        }

        private class State
        {
            public bool Completed { get; set; }
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
                        { "filename", warcId }
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
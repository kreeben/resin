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
using Sir.IO;

namespace Sir.HttpServer.Features
{
    public class CrawlJob : AsyncJob
    {
        private readonly string _directory;
        private readonly SessionFactory _sessionFactory;
        private readonly QueryParser<string> _queryParser;
        private readonly ILogger _logger;
        private readonly IModel<string> _model;
        private readonly int _skip;
        private readonly int _take;
        private readonly IIndexReadWriteStrategy _indexStrategy;
        private readonly IConfigurationProvider _config;

        public CrawlJob(
            string directory,
            SessionFactory sessionFactory,
            QueryParser<string> queryParser,
            IModel<string> model,
            IIndexReadWriteStrategy indexStrategy,
            IConfigurationProvider config,
            ILogger logger,
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
            _directory = directory;
            _sessionFactory = sessionFactory;
            _queryParser = queryParser;
            _logger = logger;
            _model = model;
            _skip = skip;
            _take = take;
            _indexStrategy = indexStrategy;
            _config = config;

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
            var writePayload = new List<Document>();

            var originalQuery = _queryParser.Parse(
                Collections, 
                Q, 
                Fields, 
                select: new string[] {"url", "title", "filename"},
                and: And, 
                or: Or,
                label: false);

            var directory = _config.Get("data_dir");
            var keys = new KeyRepository(directory, _sessionFactory);
            using (var readSession = new SearchSession(_directory, keys, _sessionFactory, _model, new LogStructuredIndexingStrategy(_model), _logger))
            {
                var originalResult = readSession.Search(originalQuery, _skip, _take)
                    .Documents
                    .ToDictionary(x => (string)x.Get("url").Value);

                var wetFileIds = new SortedList<string, object>();
                SearchResult wetResult = null;
                var wetCollectionId = "cc_wet".ToHash();

                foreach (var doc in originalResult.Values)
                {
                    var fileName = (string)doc.Get("filename").Value;
                    var wetFileId = fileName.Replace("/warc", "/wet").Replace(".gz", ".wet.gz");

                    wetFileIds.TryAdd(wetFileId, null);

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
                        or: false,
                        label: false);

                    if (wetQuery != null)
                    {
                        wetResult = readSession.Search(wetQuery, 0, 1);
                    }

                    if (wetResult == null || wetResult.Total == 0)
                    {
                        var localFileName = Path.Combine(_directory, "wet", fileName);
                        var tmpFileName = Path.Combine(_directory, "tmp", Id, fileName);

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
                            Document originalDoc;
                            var key = (string)document.Get("url").Value;

                            if (originalResult.TryGetValue(key, out originalDoc))
                            {
                                document.Get("title").Value = originalDoc.Get("title").Value;
                                document.Get("filename").Value = originalDoc.Get("filename").Value;

                                writePayload.Add(document);
                            }
                        }
                    }
                }

                Status["download"] = 100;

                if (writePayload.Count > 0)
                {
                    var time = Stopwatch.StartNew();

                    _sessionFactory.StoreDataAndPersistIndex(_directory, wetCollectionId, writePayload, _model, _indexStrategy, reportSize: 1000);

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

        private static IEnumerable<Document> ReadWetFile(string fileName, string warcId)
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
                        yield return new Document
                            (
                                new List<Field>
                                {
                                    new Field("url", url),
                                    new Field("text", content.ToString()),
                                    new Field("filename", warcId)
                                }
                            );

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
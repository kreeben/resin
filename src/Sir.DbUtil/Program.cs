using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Core;
using Sir.Document;
using Sir.Search;

namespace Sir.DbUtil
{
    class Program
    {
        static void Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder
                    .AddFilter("Microsoft", LogLevel.Warning)
                    .AddFilter("System", LogLevel.Warning)
                    .AddFilter("Sir.DbUtil.Program", LogLevel.Debug)
                    .AddConsole()
                    .AddEventLog();
            });

            var logger = loggerFactory.CreateLogger("dbutil");

            logger.LogInformation($"processing command: {string.Join(" ", args)}");

            var model = new BocModel();
            var command = args[0].ToLower();

            if (command == "submit")
            {
                var fullTime = Stopwatch.StartNew();

                Submit(args);

                logger.LogInformation("submit took {0}", fullTime.Elapsed);
            }
            else if (command == "write_wp")
            {
                var fullTime = Stopwatch.StartNew();

                WriteWP(args, model, loggerFactory);

                logger.LogInformation("write operation took {0}", fullTime.Elapsed);
            }
            else if (command == "validate")
            {
                var time = Stopwatch.StartNew();

                Validate(args, model, loggerFactory);

                logger.LogInformation("validate took {0}", time.Elapsed);
            }
            else if ((command == "slice"))
            {
                Slice(args);
            }
            else if (command == "download_cc")
            {
                DownloadAndIndexCC(args, model, loggerFactory, logger);
            }
            else if (command == "truncate")
            {
                Truncate(args, model, loggerFactory);
            }
            else
            {
                logger.LogInformation("unknown command: {0}", command);
            }

            logger.LogInformation($"executed {command}");
        }

        private static void DownloadAndIndexCC(string[] args, BocModel model, ILoggerFactory logger, ILogger log)
        {
            var ccName = args[1];
            var workingDir = args[2];
            var collection = args[3];
            var fileName = $"{ccName}/wat.paths.gz";
            var localFileName = Path.Combine(workingDir, fileName);

            if (!File.Exists(localFileName))
            {
                var remotePath = $"https://commoncrawl.s3.amazonaws.com/crawl-data/{fileName}";

                log.LogInformation($"downloading {remotePath}");

                if (!Directory.Exists(Path.GetDirectoryName(localFileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(localFileName));
                }

                using (var client = new WebClient())
                {
                    client.DownloadFile(remotePath, localFileName);
                }

                log.LogInformation($"downloaded {localFileName}");
            }

            log.LogInformation($"processing {localFileName}");

            Task writeTask = null;

            foreach (var watFileName in ReadAllLinesGromGz(localFileName))
            {
                var local = Path.Combine(workingDir, watFileName);

                if (!File.Exists(local))
                {
                    var remotePath = $"https://commoncrawl.s3.amazonaws.com/{watFileName}";

                    log.LogInformation($"downloading {remotePath}");

                    if (!Directory.Exists(Path.GetDirectoryName(local)))
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(local));
                    }

                    using (var client = new WebClient())
                    {
                        client.DownloadFile(remotePath, local);
                    }
                }

                log.LogInformation($"processing {local}");

                var warcFileName = watFileName.Replace(".wat", ".gz");

                if (writeTask != null)
                {
                    log.LogInformation($"synchronizing write");

                    writeTask.Wait();
                }

                writeTask = Task.Run(
                    () => WriteCC(local, collection, model, logger, log, warcFileName));
            }
        }

        private static void WriteCC(
            string fileName, 
            string collection, 
            IStringModel model, 
            ILoggerFactory log, 
            ILogger logger,
            string warcFileName)
        {
            var documents = ReadCC(fileName, warcFileName);
            var collectionId = collection.ToHash();
            const int reportSize = 1000;
            var time = Stopwatch.StartNew();
            var batchNo = 0;

            using (var sessionFactory = new SessionFactory(new IniConfiguration("sir.ini"), model, log))
            {
                using (var writeSession = sessionFactory.CreateWriteSession(collectionId, model))
                {
                    using(var queue = new ProducerConsumerQueue<IList<IDictionary<string, object>>>(1,
                        (batch) =>
                        {
                            var batchTime = Stopwatch.StartNew();

                            foreach (var document in batch)
                            {
                                writeSession.Write(document);
                            }

                            var info = writeSession.GetIndexInfo();
                            var t = batchTime.Elapsed.TotalMilliseconds;
                            var docsPerSecond = (int)(reportSize / t * 1000);
                            var debug = string.Join('\n', info.Info.Select(x => x.ToString()));

                            logger.LogInformation(
                                $"batch {batchNo++} took {batchTime.ElapsedMilliseconds} ms. {docsPerSecond} docs/s");

                            logger.LogInformation($"\t{debug}");
                        }))
                    {
                        foreach (var batch in documents.Batch(reportSize))
                        {
                            queue.Enqueue(batch.ToList());
                        }
                    }
                }
            }

            logger.LogInformation($"indexed {fileName} in {time.Elapsed}");
        }

        private static IEnumerable<IDictionary<string, object>> ReadCC(string fileName, string warcFileName)
        {
            using (var fs = File.OpenRead(fileName))
            using (var zip = new GZipStream(fs, CompressionMode.Decompress))
            using (var reader = new StreamReader(zip, Encoding.UTF8))
            {
                var line = reader.ReadLine();

                while (line != null)
                {
                    if (line.StartsWith('{'))
                    {
                        var doc = JsonConvert.DeserializeObject<Dictionary<string, object>>(
                            line,
                            new JsonConverter[] { new DictionaryConverter() });

                        var envelope = (Dictionary<string, object>)doc["Envelope"];
                        var header = (Dictionary<string, object>)envelope["WARC-Header-Metadata"];
                        var type = (string)header["WARC-Type"];

                        if (type == "response")
                        {
                            var payloadMetaData = (Dictionary<string, object>)envelope["Payload-Metadata"];
                            var response = (Dictionary<string, object>)payloadMetaData["HTTP-Response-Metadata"];
                            var url = new Uri(Uri.UnescapeDataString((string)header["WARC-Target-URI"]));
                            string title = null;
                            string description = null;

                            if (response.ContainsKey("HTML-Metadata"))
                            {
                                var htmlMetaData = (Dictionary<string, object>)response["HTML-Metadata"];

                                if (htmlMetaData.ContainsKey("Head"))
                                {
                                    var head = (Dictionary<string, object>)htmlMetaData["Head"];

                                    if (head.ContainsKey("Title"))
                                    {
                                        title = (string)head["Title"];
                                    }

                                    if (head.ContainsKey("Metas"))
                                    {
                                        foreach (var meta in (IEnumerable<dynamic>)head["Metas"])
                                        {
                                            foreach (var prop in meta)
                                            {
                                                bool hasDescription = false;

                                                foreach (var x in prop)
                                                {
                                                    if (x.Value as string == "description")
                                                    {
                                                        hasDescription = true;
                                                    }
                                                }

                                                if (hasDescription && prop.Next != null)
                                                {
                                                    description = prop.Next.Value.ToString();
                                                }
                                            }
                                        }
                                    }
                                }
                            }

                            yield return new Dictionary<string, object>
                                {
                                    { "title", title },
                                    { "description", description },
                                    { "scheme", url.Scheme },
                                    { "host", url.Host },
                                    { "path", url.AbsolutePath },
                                    { "query", url.Query },
                                    { "url", url.ToString() },
                                    { "filename", warcFileName}
                                };
                        }
                    }

                    line = reader.ReadLine();
                }
            }
        }

        private static void Slice(string[] args)
        {
            var file = args[1];
            var slice = args[2];
            var len = int.Parse(args[3]);
            Span<byte> buf = new byte[len];

            using (var fs = File.OpenRead(file))
            using (var target = File.Create(slice))
            {
                fs.Read(buf);
                target.Write(buf);
            }
        }

        private static void Validate(string[] args, IStringModel model, ILoggerFactory log)
        {
            var collection = args[1];
            var skip = int.Parse(args[2]);
            var take = int.Parse(args[3]);
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(new IniConfiguration("sir.ini"), model, log))
            {
                using (var validateSession = sessionFactory.CreateValidateSession(collectionId))
                using (var documents = new DocumentStreamSession(new DocumentReader(collectionId, sessionFactory)))
                {
                    foreach (var doc in documents.ReadDocs(skip, take))
                    {
                        validateSession.Validate(doc);

                        Console.WriteLine(doc["___docid"]);
                    }
                }
            }
        }

        private static void WriteWP(string[] args, IStringModel model, ILoggerFactory log)
        {
            var fileName = args[1];
            var dir = args[2];
            var collection = args[3];
            var skip = int.Parse(args[4]);
            var take = int.Parse(args[5]);
            var pageSize = int.Parse(args[6]);
            const int reportSize = 1000;
            var collectionId = collection.ToHash();
            var batchNo = 0;
            var payload = ReadWP(fileName, skip, take)
                .Select(x => new Dictionary<string, object>
                        {
                                { "_language", x["language"].ToString() },
                                { "_url", string.Format("www.wikipedia.org/search-redirect.php?family=wikipedia&language={0}&search={1}", x["language"], x["title"]) },
                                { "title", x["title"] },
                                { "body", x["text"] }
                        });

            using (var sessionFactory = new SessionFactory(new IniConfiguration("sir.ini"), model, log))
            {
                foreach (var page in payload.Batch(pageSize))
                {
                    using (var writeSession = sessionFactory.CreateWriteSession(collectionId, model))
                    {
                        foreach (var batch in page.Batch(reportSize))
                        {
                            var time = Stopwatch.StartNew();

                            foreach (var document in page)
                            {
                                writeSession.Write(document);
                            }

                            var info = writeSession.GetIndexInfo();
                            var t = time.Elapsed.TotalMilliseconds;
                            var docsPerSecond = (int)(pageSize / t * 1000);

                            foreach (var stat in info.Info)
                            {
                                if (stat.Weight > 500)
                                    Console.WriteLine(stat);
                            }

                            Console.WriteLine(
                                    $"batch {batchNo++} took {t} ms. {docsPerSecond} docs/s");
                        }
                    }
                }
            }
        }

        private static void Truncate(string[] args, IStringModel model, ILoggerFactory log)
        {
            var collection = args[1];
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(new IniConfiguration("sir.ini"), model, log))
            {
                sessionFactory.Truncate(collectionId);
            }
        }

        private static void Submit(string[] args)
        {
            var fileName = args[1];
            var uri = new Uri(args[2]);
            var count = int.Parse(args[3]);
            var batchSize = int.Parse(args[4]);
            var batchNo = 0;
            using (var httpClient = new HttpClient())
            {
                var payload = ReadWP(fileName, 0, count)
                                        .Select(x => new Dictionary<string, object>
                                                {
                                { "_language", x["language"].ToString() },
                                { "_url", string.Format("www.wikipedia.org/search-redirect.php?family=wikipedia&language={0}&search={1}", x["language"], x["title"]) },
                                { "title", x["title"] },
                                { "body", x["text"] }
                                                });

                foreach (var batch in payload.Batch(batchSize))
                {
                    var time = Stopwatch.StartNew();
                    Submit(batch, uri, httpClient);
                    time.Stop();
                    var docsPerSecond = (int)(batchSize / time.Elapsed.TotalSeconds);
                    Console.WriteLine($"batch {batchNo++} took {time.Elapsed} {docsPerSecond} docs/s");
                }
            }
        }

        private static IEnumerable<IDictionary> ReadGZipJsonFile(string fileName, int skip, int take)
        {
            var skipped = 0;
            var took = 0;

            using (var stream = File.OpenRead(fileName))
            using (var zip = new GZipStream(stream, CompressionMode.Decompress))
            using (var reader = new StreamReader(zip))
            {
                //skip first line
                reader.ReadLine();

                var line = reader.ReadLine();

                while (!string.IsNullOrWhiteSpace(line) && !line.StartsWith("]"))
                {
                    if (took == take)
                        break;

                    if (skipped++ < skip)
                    {
                        continue;
                    }

                    var doc = JsonConvert.DeserializeObject<IDictionary>(line);

                    if (doc.Contains("title"))
                    {
                        yield return doc;
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }

        private static IEnumerable<string> ReadAllLinesGromGz(string fileName)
        {
            using (var stream = File.OpenRead(fileName))
            using (var zip = new GZipStream(stream, CompressionMode.Decompress))
            using (var reader = new StreamReader(zip))
            {
                var line = reader.ReadLine();

                while (!string.IsNullOrWhiteSpace(line))
                {
                    yield return line;

                    line = reader.ReadLine();
                }
            }
        }

        private static IEnumerable<IDictionary> ReadWP(string fileName, int skip, int take)
        {
            if (Path.GetExtension(fileName).EndsWith("gz"))
            {
                return ReadGZipJsonFile(fileName, skip, take);
            }

            return ReadJsonFile(fileName, skip, take);
        }

        private static IEnumerable<IDictionary> ReadJsonFile(string fileName, int skip, int take)
        {
            var skipped = 0;
            var took = 0;

            using (var stream = File.OpenRead(fileName))
            using (var reader = new StreamReader(stream))
            {
                //skip first line
                reader.ReadLine();

                var line = reader.ReadLine();

                while (!string.IsNullOrWhiteSpace(line) && !line.StartsWith("]"))
                {
                    if (took == take)
                        break;

                    if (skipped++ < skip)
                    {
                        continue;
                    }

                    var doc = JsonConvert.DeserializeObject<IDictionary>(line);

                    if (doc.Contains("title"))
                    {
                        yield return doc;
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }

        private static void Submit(IEnumerable<IDictionary> documents, Uri uri, HttpClient client)
        {
            var jsonStr = JsonConvert.SerializeObject(documents);
            var content = new StringContent(jsonStr, Encoding.UTF8, "application/json");
            var response = client.PostAsync(uri, content).Result;

            response.EnsureSuccessStatusCode();
        }

        private static void Serialize(IEnumerable<object> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }
}


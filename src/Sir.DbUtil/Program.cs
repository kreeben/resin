using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.CommonCrawl;
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
                    .AddConsole().AddDebug();
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
            else if ((command == "slice"))
            {
                Slice(args);
            }
            else if (command == "download_wat")
            {
                // Ex: download_wat CC-MAIN-2019-51 d:\ cc_wat 0 1

                CCHelper.DownloadAndIndexWat(
                    commonCrawlId:args[1],
                    workingDirectory:args[2],
                    collectionName:args[3],
                    skip: int.Parse(args[4]),
                    take: int.Parse(args[5]),
                    model, 
                    logger);
            }
            else if (command == "download_embedd_wat")
            {
                // Ex: download_embedd_wat CC-MAIN-2019-51 d:\ cc_wat 0 1

                CCHelper.DownloadAndCreateWatEmbeddings(
                    commonCrawlId: args[1],
                    workingDirectory: args[2],
                    collectionName: args[3],
                    skip: int.Parse(args[4]),
                    take: int.Parse(args[5]),
                    model,
                    logger);
            }
            else if (command == "write_wet")
            {
                // Ex: D:\CC-MAIN-2019-43\segments\1570986647517.11\wet\CC-MAIN-20191013195541-20191013222541-00000.warc.wet.gz
                WriteWet(args, model, logger);
            }
            else if (command == "truncate")
            {
                Truncate(args, model, logger);
            }
            else if (command == "truncate-index")
            {
                TruncateIndex(args, model, logger);
            }
            else if (command == "optimize")
            {
                Optimize(args, model, logger);
            }
            else
            {
                logger.LogInformation("unknown command: {0}", command);
            }

            logger.LogInformation($"executed {command}");
        }

        private static void Optimize(string[] args, BocModel model, ILogger logger)
        {
            var collection = args[1];
            var skip = int.Parse(args[2]);
            var take = int.Parse(args[3]);
            var batchSize = int.Parse(args[4]);

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, logger))
            {
                sessionFactory.Optimize(
                    collection, 
                    new HashSet<string> { "title", "description", "url", "filename" },
                    new HashSet<string> { "title", "description", "url" },
                    skip,
                    take,
                    batchSize);
            }
        }

        private static void WriteWet(string[] args, IStringModel model, ILogger logger)
        {
            var fileName = args[1];
            var collectionId = "cc_wet".ToHash();
            var storedFieldNames = new HashSet<string> { "url" };
            var indexedFieldNames = new HashSet<string> { "description" };

            var writeJob = new WriteJob(
                collectionId, 
                ReadWetFile(fileName), 
                model,
                storedFieldNames,
                indexedFieldNames);

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, logger))
            {
                sessionFactory.Truncate(collectionId);

                sessionFactory.Write(writeJob, reportSize:1000);
            }
        }

        private static IEnumerable<IDictionary<string, object>> ReadWetFile(string fileName)
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

        private static void WriteWP(string[] args, IStringModel model, ILoggerFactory log)
        {
            //var fileName = args[1];
            //var dir = args[2];
            //var collection = args[3];
            //var skip = int.Parse(args[4]);
            //var take = int.Parse(args[5]);
            //var pageSize = int.Parse(args[6]);
            //const int reportSize = 1000;
            //var collectionId = collection.ToHash();
            //var batchNo = 0;
            //var payload = ReadWP(fileName, skip, take)
            //    .Select(x => new Dictionary<string, object>
            //            {
            //                    { "_language", x["language"].ToString() },
            //                    { "_url", string.Format("www.wikipedia.org/search-redirect.php?family=wikipedia&language={0}&search={1}", x["language"], x["title"]) },
            //                    { "title", x["title"] },
            //                    { "body", x["text"] }
            //            });

            //using (var sessionFactory = new SessionFactory(new IniConfiguration("sir.ini"), model, log))
            //{
            //    foreach (var page in payload.Batch(pageSize))
            //    {
            //        using (var writeSession = sessionFactory.CreateWriteSession(collectionId, model))
            //        {
            //            foreach (var batch in page.Batch(reportSize))
            //            {
            //                var time = Stopwatch.StartNew();

            //                foreach (var document in page)
            //                {
            //                    writeSession.Write(document);
            //                }

            //                var info = writeSession.GetIndexInfo();
            //                var t = time.Elapsed.TotalMilliseconds;
            //                var docsPerSecond = (int)(pageSize / t * 1000);

            //                foreach (var stat in info.Info)
            //                {
            //                    if (stat.Weight > 500)
            //                        Console.WriteLine(stat);
            //                }

            //                Console.WriteLine(
            //                        $"batch {batchNo++} took {t} ms. {docsPerSecond} docs/s");
            //            }
            //        }
            //    }
            //}
        }

        private static void Truncate(string[] args, IStringModel model, ILogger log)
        {
            var collection = args[1];
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, log))
            {
                sessionFactory.Truncate(collectionId);
            }
        }

        private static void TruncateIndex(string[] args, IStringModel model, ILogger log)
        {
            var collection = args[1];
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, log))
            {
                sessionFactory.TruncateIndex(collectionId);
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


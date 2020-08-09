using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Search;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Sir.CommonCrawl
{
    public static class CCHelper
    {
        public static void DownloadAndCreateWatEmbeddings(
            string commonCrawlId,
            string workingDirectory,
            string collectionName,
            int skip,
            int take,
            IStringModel model,
            ILogger log)
        {
            var pathsFileName = $"{commonCrawlId}/wat.paths.gz";
            var localPathsFileName = Path.Combine(workingDirectory, pathsFileName);

            if (!File.Exists(localPathsFileName))
            {
                var url = $"https://commoncrawl.s3.amazonaws.com/crawl-data/{pathsFileName}";

                log.LogInformation($"downloading {url}");

                if (!Directory.Exists(Path.GetDirectoryName(localPathsFileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(localPathsFileName));
                }

                using (var client = new WebClient())
                {
                    client.DownloadFile(url, localPathsFileName);
                }

                log.LogInformation($"downloaded {localPathsFileName}");
            }

            log.LogInformation($"processing {localPathsFileName}");

            Task writeTask = null;
            var took = 0;
            var skipped = 0;
            var embeddingsCollectionId = (collectionName + "embeddings").ToHash();

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, log))
            using (var writeSession = sessionFactory.CreateWriteSession(embeddingsCollectionId))
            using (var indexSession = sessionFactory.CreateWordEmbeddingsSession(embeddingsCollectionId))
            foreach (var watFileName in ReadAllLinesGromGz(localPathsFileName))
            {
                if (skip > skipped)
                {
                    skipped++;
                    continue;
                }

                if (took++ == take)
                {
                    break;
                }

                var localWatFileName = Path.Combine(workingDirectory, watFileName);

                if (!File.Exists(localWatFileName))
                {
                    var url = $"https://commoncrawl.s3.amazonaws.com/{watFileName}";

                    log.LogInformation($"downloading {url}");

                    if (!Directory.Exists(Path.GetDirectoryName(localWatFileName)))
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(localWatFileName));
                    }

                    using (var client = new WebClient())
                    {
                        client.DownloadFile(url, localWatFileName);
                    }

                    log.LogInformation($"downloaded {localWatFileName}");
                }

                var refFileName = watFileName.Replace(".wat", "").Replace("/wat", "/warc");

                //log.LogInformation($"processing {localWatFileName}");
                //WriteWatSegment(localWatFileName, collection, model, logger, log, refFileName);

                if (writeTask != null)
                {
                    log.LogInformation($"synchronizing write");

                    writeTask.Wait();
                }

                writeTask = Task.Run(() =>
                {
                    log.LogInformation($"processing {localWatFileName}");

                    WriteEmbeddings(sessionFactory, writeSession, indexSession, localWatFileName, collectionName, model, log, refFileName);
                });
            }
        }

        public static void DownloadAndIndexWat(
            string commonCrawlId,
            string workingDirectory,
            string collectionName,
            int skip,
            int take,
            IStringModel model, 
            ILogger log)
        {
            var pathsFileName = $"{commonCrawlId}/wat.paths.gz";
            var localPathsFileName = Path.Combine(workingDirectory, pathsFileName);

            if (!File.Exists(localPathsFileName))
            {
                var url = $"https://commoncrawl.s3.amazonaws.com/crawl-data/{pathsFileName}";

                log.LogInformation($"downloading {url}");

                if (!Directory.Exists(Path.GetDirectoryName(localPathsFileName)))
                {
                    Directory.CreateDirectory(Path.GetDirectoryName(localPathsFileName));
                }

                using (var client = new WebClient())
                {
                    client.DownloadFile(url, localPathsFileName);
                }

                log.LogInformation($"downloaded {localPathsFileName}");
            }

            log.LogInformation($"processing {localPathsFileName}");

            Task writeTask = null;
            var took = 0;
            var skipped = 0;

            foreach (var watFileName in ReadAllLinesGromGz(localPathsFileName))
            {
                if (skip > skipped)
                {
                    skipped++;
                    continue;
                }

                if (took++ == take)
                {
                    break;
                }

                var localWatFileName = Path.Combine(workingDirectory, watFileName);

                if (!File.Exists(localWatFileName))
                {
                    var url = $"https://commoncrawl.s3.amazonaws.com/{watFileName}";

                    log.LogInformation($"downloading {url}");

                    if (!Directory.Exists(Path.GetDirectoryName(localWatFileName)))
                    {
                        Directory.CreateDirectory(Path.GetDirectoryName(localWatFileName));
                    }

                    using (var client = new WebClient())
                    {
                        client.DownloadFile(url, localWatFileName);
                    }

                    log.LogInformation($"downloaded {localWatFileName}");
                }

                var refFileName = watFileName.Replace(".wat", "").Replace("/wat", "/warc");

                //log.LogInformation($"processing {localWatFileName}");
                //WriteWatSegment(localWatFileName, collection, model, logger, log, refFileName);

                if (writeTask != null)
                {
                    log.LogInformation($"synchronizing write");

                    writeTask.Wait();
                }

                writeTask = Task.Run(() =>
                {
                    log.LogInformation($"processing {localWatFileName}");

                    WriteWatSegment(localWatFileName, collectionName, model, log, refFileName);
                });
            }
        }

        private static void WriteEmbeddings(
            SessionFactory sessionFactory,
            WriteSession writeSession,
            WordEmbeddingsSession indexSession,
            string fileName,
            string collection,
            IStringModel model,
            ILogger logger,
            string refFileName)
        {
            var documents = ReadWatFile(fileName, refFileName);
            var collectionId = collection.ToHash();
            var time = Stopwatch.StartNew();
            var storedFieldNames = new HashSet<string>();
            var indexedFieldNames = new HashSet<string>
            {
                "title","description", "url"
            };

            sessionFactory.CreateWordEmbeddings(
                            new WriteJob(
                                collectionId,
                                documents,
                                model,
                                storedFieldNames,
                                indexedFieldNames),
                            writeSession,
                            indexSession,
                            reportSize: 1000);

            logger.LogInformation($"indexed {fileName} in {time.Elapsed}");
        }

        private static void WriteWatSegment(
            string fileName,
            string collection,
            IStringModel model,
            ILogger logger,
            string refFileName)
        {
            var documents = ReadWatFile(fileName, refFileName);
            var collectionId = collection.ToHash();
            var time = Stopwatch.StartNew();
            var storedFieldNames = new HashSet<string>
            {
                "title","description", "url", "filename"
            };
            var indexedFieldNames = new HashSet<string>
            {
                "title","description", "url"
            };

            using (var sessionFactory = new SessionFactory(new KeyValueConfiguration("sir.ini"), model, logger))
            {
                sessionFactory.Write(
                            new WriteJob(
                                collectionId,
                                documents,
                                model,
                                storedFieldNames,
                                indexedFieldNames),
                            reportSize: 1000);
            }

            logger.LogInformation($"indexed {fileName} in {time.Elapsed}");
        }

        private static IEnumerable<IDictionary<string, object>> ReadWatFile(string fileName, string refFileNae)
        {
            using (var fs = new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan))
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
                                    { "filename", refFileNae}
                                };
                        }
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
    }
}

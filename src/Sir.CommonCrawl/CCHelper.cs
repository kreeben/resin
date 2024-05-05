using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Sir.Core;
using Sir.Documents;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Sir.CommonCrawl
{
    public static class CCHelper
    {
        public static void WriteWatSegment(
            string dataDirectory,
            string fileName,
            string collection,
            IModel<string> model,
            ILogger logger,
            string refFileName)
        {
            var time = Stopwatch.StartNew();
            var collectionId = collection.ToHash();
            var storeFields = new HashSet<string>
            {
                "title","description", "url", "filename"
            };
            var indexFields = new HashSet<string>
            {
                "title","description", "url"
            };

            using (var sessionFactory = new SessionFactory(logger))
            using (var writeSession = new WriteSession(new DocInfoWriter(dataDirectory, collectionId)))
            using (var indexSession = new IndexSession<string>(model, new LogStructuredIndexingStrategy(model), dataDirectory, collectionId, logger))
            {
                using (var queue = new ProducerConsumerQueue<Document>(document =>
                {
                    sessionFactory.StoreDataAndBuildInMemoryIndex(document, writeSession, indexSession);
                }))
                {
                    foreach (var document in ReadWatFile(fileName, refFileName).Select(dic =>
                            new Document(
                                dic.Select(kvp => new Field(
                                    kvp.Key,
                                    kvp.Value)))))
                    {
                        queue.Enqueue(document);
                    }
                }

                indexSession.Commit();
            }

            logger.LogInformation($"indexed {fileName} in {time.Elapsed}");
        }

        public static IEnumerable<IDictionary<string, object>> ReadWatFile(string fileName, string refFileNae)
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

        public static IEnumerable<string> ReadAllLinesGromGz(string fileName)
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

    /// <summary>
    /// https://stackoverflow.com/questions/6416017/json-net-deserializing-nested-dictionaries
    /// </summary>
    public class DictionaryConverter : CustomCreationConverter<IDictionary<string, object>>
    {
        public override IDictionary<string, object> Create(Type objectType)
        {
            return new Dictionary<string, object>();
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(object) || base.CanConvert(objectType);
        }

        public override object ReadJson(
            JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            if (reader.TokenType == JsonToken.StartObject
                || reader.TokenType == JsonToken.Null)
                return base.ReadJson(reader, objectType, existingValue, serializer);

            return serializer.Deserialize(reader);
        }
    }
}

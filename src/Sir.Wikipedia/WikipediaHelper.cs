using Newtonsoft.Json.Linq;
using Sir.Documents;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace Sir.Wikipedia
{
    public static class WikipediaHelper
    {
        public static IEnumerable<Document> Read(string fileName, int skip, int take, HashSet<string> fieldsOfInterest, string urlFormat = "https://en.wikipedia.org/wiki/{0}")
        {
            return ReadGZipJsonFile(fileName, skip, take, fieldsOfInterest, urlFormat);
        }

        public static IEnumerable<Document> ReadGZipJsonFile(string fileName, int skip, int take, HashSet<string> fieldsOfInterest, string urlFormat)
        {
            using (var stream = File.OpenRead(fileName))
            using (var zip = new GZipStream(stream, CompressionMode.Decompress))
            using (var reader = new StreamReader(zip))
            {
                var skipped = 0;
                var took = 0;

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

                    var jobject = JObject.Parse(line);

                    var fields = new List<Field>();

                    foreach (var kvp in jobject)
                    {
                        if (fieldsOfInterest.Contains(kvp.Key))
                        {
                            fields.Add(new Field(kvp.Key, kvp.Value.ToString()));
                        }
                    }

                    if (fields.Count > 0)
                    {
                        if (fieldsOfInterest.Contains("url"))
                        {
                            var url = string.Format(urlFormat, Uri.EscapeDataString(jobject["title"].ToString()));
                            var uri = new Uri(url);

                            fields.Add(new Field("url", uri.ToString()));
                        }

                        yield return new Document(fields);
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }

        public static IEnumerable<Document> ReadJsonFile(string fileName, int skip, int take, HashSet<string> fieldsOfInterest)
        {
            using (var stream = File.OpenRead(fileName))
            using (var reader = new StreamReader(stream))
            {
                var skipped = 0;
                var took = 0;

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

                    var jobject = JObject.Parse(line);

                    if (jobject.ContainsKey("title"))
                    {
                        var fields = new List<Field>();

                        foreach (var kvp in jobject)
                        {
                            if (fieldsOfInterest.Contains(kvp.Key))
                                fields.Add(new Field(kvp.Key, kvp.Value.Value<object>()));
                        }

                        yield return new Document(fields);
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }
    }
}
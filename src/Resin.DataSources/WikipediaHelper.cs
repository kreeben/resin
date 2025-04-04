using System.IO.Compression;
using Newtonsoft.Json.Linq;

namespace Sir.Wikipedia
{
    public static class WikipediaHelper
    {
        public static IEnumerable<(string key, IEnumerable<string> values)> Read(string fileName, int skip, int take, HashSet<string> fieldsOfInterest)
        {
            return fieldsOfInterest.Select(field => (field, ReadGZipJsonFile(fileName, skip, take, field)));
        }

        public static IEnumerable<string> ReadGZipJsonFile(string fileName, int skip, int take, string fieldOfInterest, string urlFormat = "https://en.wikipedia.org/wiki/{0}")
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

                    foreach (var kvp in jobject)
                    {
                        if (kvp.Value != null && fieldOfInterest.Equals(kvp.Key))
                        {
                            yield return kvp.Value.ToString();
                        }
                    }
                    if (fieldOfInterest.Equals("url"))
                    {
                        var url = string.Format(urlFormat, Uri.EscapeDataString(jobject["title"].ToString()));
                        var uri = new Uri(url);

                        yield return uri.ToString();
                    }
                    took++;
                    line = reader.ReadLine();
                }
            }
        }

        public static IEnumerable<ISet<(string key, string value)>> ReadGZipJsonFile(string fileName, int skip, int take, HashSet<string> fieldsOfInterest, string urlFormat = "https://en.wikipedia.org/wiki/{0}")
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

                    var fields = new HashSet<(string key, string value)>();

                    foreach (var kvp in jobject)
                    {
                        if (kvp.Value != null && fieldsOfInterest.Contains(kvp.Key))
                        {
                            fields.Add((kvp.Key, kvp.Value.ToString()));
                        }
                    }

                    if (fields.Count > 0)
                    {
                        if (fieldsOfInterest.Contains("url"))
                        {
                            var url = string.Format(urlFormat, Uri.EscapeDataString(jobject["title"].ToString()));
                            var uri = new Uri(url);

                            fields.Add(("url", uri.ToString()));
                        }

                        yield return fields;
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }
    }
}
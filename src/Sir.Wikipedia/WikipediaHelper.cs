﻿using Newtonsoft.Json.Linq;
using Sir.Search;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;

namespace Sir.Wikipedia
{
    public static class WikipediaHelper
    {
        public static IEnumerable<Document> ReadWP(string fileName, int skip, int take, HashSet<string> fieldsToStore, HashSet<string> fieldsToIndex)
        {
            return ReadGZipJsonFile(fileName, skip, take, fieldsToStore, fieldsToIndex);
        }

        public static IEnumerable<Document> ReadGZipJsonFile(
            string fileName, int skip, int take, HashSet<string> fieldsToStore, HashSet<string> fieldsToIndex)
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

                    if (jobject.ContainsKey("title"))
                    {
                        var fields = new List<Field>();

                        foreach (var kvp in jobject)
                        {
                            var store = fieldsToStore.Contains(kvp.Key);
                            var index = fieldsToIndex.Contains(kvp.Key);

                            if (store || index)
                                fields.Add(new Field(kvp.Key, kvp.Value.ToString(), index:index, store:store));
                        }

                        fields.Add(
                            new Field(
                                "url", 
                                $"https://www.wikidata.org/wiki/{jobject["wikibase_item"]}", 
                                index:false, 
                                store:true));

                        yield return new Document(fields);
                        took++;
                    }

                    line = reader.ReadLine();
                }
            }
        }

        public static IEnumerable<Document> ReadJsonFile(string fileName, int skip, int take, HashSet<string> fieldsToStore, HashSet<string> fieldsToIndex)
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
                            var store = fieldsToStore.Contains(kvp.Key);
                            var index = fieldsToIndex.Contains(kvp.Key);

                            if (store || index)
                                fields.Add(new Field(kvp.Key, kvp.Value.Value<object>(), index:index, store:store));
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
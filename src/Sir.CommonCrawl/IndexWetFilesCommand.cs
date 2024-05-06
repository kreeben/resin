using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.Strings;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;

namespace Sir.CommonCrawl
{
    public class IndexWetFilesCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["dataDirectory"];
            var fileName = args["fileName"];
            var model = new BagOfCharsModel();
            var indexStrategy = new LogStructuredIndexingStrategy(model);
            var collectionId = "cc_wet".ToHash();
            var storeFields = new HashSet<string> { "url" };
            var indexFields = new HashSet<string> { "description" };

            using (var sessionFactory = new SessionFactory(logger))
            {
                sessionFactory.Truncate(dataDirectory, collectionId);

                sessionFactory.StoreDataAndPersistIndex(
                    dataDirectory,
                    collectionId,
                    ReadWetFile(fileName)
                                .Select(dic =>
                                    new Document(
                                        dic.Select(kvp => new Field(
                                            kvp.Key,
                                            kvp.Value)).ToList())),
                    model,
                    indexStrategy,
                    reportSize: 1000);
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
    }
}

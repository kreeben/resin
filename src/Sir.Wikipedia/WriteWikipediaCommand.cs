using Microsoft.Extensions.Logging;
using Sir.Documents;
using System.Collections.Generic;
using System.IO;

namespace Sir.Wikipedia
{
    /// <summary>
    /// Download JSON search index dump here: 
    /// https://dumps.wikimedia.org/other/cirrussearch/current/
    /// </summary>
    /// <example>
    /// writewikipedia --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --file d:\enwiki-20211122-cirrussearch-content.json.gz --collection wikipedia --skip 0 --take 1000
    /// </example>
    public class WriteWikipediaCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["directory"];
            var fileName = args["file"];
            var collection = args["collection"];
            var skip = args.ContainsKey("skip") ? int.Parse(args["skip"]) : 0;
            var take = args.ContainsKey("take") ? int.Parse(args["take"]) : int.MaxValue;
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;

            if (!File.Exists(fileName))
            {
                throw new FileNotFoundException($"This file could not be found: {fileName}. Download a wikipedia JSON dump here:  https://dumps.wikimedia.org/other/cirrussearch/current/");
            }

            var collectionId = collection.ToHash();
            var fieldsOfInterest = new HashSet<string> { "title", "text", "url" };

            if (take == 0)
                take = int.MaxValue;

            var payload = WikipediaHelper.Read(fileName, skip, take, fieldsOfInterest);

            using (var sessionFactory = new SessionFactory(logger))
            {
                var debugger = new BatchDebugger("write session", logger, sampleSize);

                using (var writeSession = new WriteSession(new DocumentWriter(sessionFactory, dataDirectory, collectionId)))
                {
                    foreach (var document in payload)
                    {
                        writeSession.Put(document);

                        debugger.Step();
                    }
                }
            }
        }
    }
}
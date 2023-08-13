using Microsoft.Extensions.Logging;
using Sir.IO;
using Sir.Strings;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir.Cmd
{
    public class ValidateCommand : ICommand
    {
        /// <summary>
        /// E.g. validate --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --collection wikipedia --skip 0 --take 1000
        /// </summary>
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = args["directory"];
            var collection = args["collection"];
            var skip = int.Parse(args["skip"]);
            var take = int.Parse(args["take"]);
            var collectionId = collection.ToHash();
            var model = new BagOfCharsModel();
            var selectFields = new HashSet<string> { "title" };
            var time = Stopwatch.StartNew();
            var count = 0;
            var embedding = new SortedList<int, float>();
            var keys = new KeyRepository();

            using (var sessionFactory = new SessionFactory(logger))
            using (var validateSession = new ValidateSession<string>(
                    collectionId,
                    new SearchSession(dir, keys, sessionFactory, model, new LogStructuredIndexingStrategy(model), logger),
                    new QueryParser<string>(dir, keys, model, embedding: embedding, logger: logger)))
            {
                using (var documents = new DocumentStreamSession(sessionFactory, keys, dir))
                {
                    foreach (var doc in documents.ReadDocuments(collectionId, selectFields, skip, take))
                    {
                        validateSession.Validate(doc);
                        count++;
                        Console.WriteLine($"{doc.Id} {doc.Get("title").Value}");
                    }
                }
            }

            Console.WriteLine($"validated {count} docs in {time.Elapsed}");
        }
    }
}
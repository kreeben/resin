using Microsoft.Extensions.Logging;
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
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;

            using (var sessionFactory = new SessionFactory(logger))
            {
                using (var debugger = new BatchDebugger(logger, sampleSize))
                using (var validateSession = new ValidateSession<string>(
                    collectionId, 
                    new SearchSession(dir, sessionFactory, model, new LogStructuredIndexingStrategy(model), logger), 
                    new QueryParser<string>(dir, sessionFactory, model, embedding: embedding, logger: logger)))
                {
                    using (var documents = new DocumentStreamSession(dir, sessionFactory))
                    {
                        foreach (var doc in documents.ReadDocuments(collectionId, selectFields, skip, take))
                        {
                            validateSession.Validate(doc);

                            debugger.Step("validating documents");
                            logger.LogInformation($"{doc.Id} {doc.Get("title").Value}");

                            count++;
                        }
                    }
                }
            }

            Console.WriteLine($"successfully validated {count} docs in {time.Elapsed}");
        }
    }
}
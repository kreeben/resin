﻿using Microsoft.Extensions.Logging;
using Sir.IO;
using Sir.Strings;
using System.Collections.Generic;
using System.IO;

namespace Sir.Wikipedia
{
    /// <example>
    /// indexwikipedia --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --collection wikipedia --skip 0 --take 1000
    /// </example>
    public class IndexWikipediaCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            // Required
            var dataDirectory = args["directory"];
            var collection = args["collection"];

            // Optional
            var skip = args.ContainsKey("skip") ? int.Parse(args["skip"]) : 0;
            var take = args.ContainsKey("take") ? int.Parse(args["take"]) : int.MaxValue;
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;
            var pageSize = args.ContainsKey("pageSize") ? int.Parse(args["pageSize"]) : 100000;

            var collectionId = collection.ToHash();
            var fieldsOfInterest = new HashSet<string> { "title", "text", "url" };

            if (take == 0)
                take = int.MaxValue;

            var model = new BagOfCharsModel();
            var indexStrategy = new LogStructuredIndexingStrategy(model);

            using (var streamDispatcher = new SessionFactory(logger))
            {
                using (var debugger = new IndexDebugger(logger, sampleSize))
                using (var documents = new DocumentStreamSession(dataDirectory, streamDispatcher))
                using (var indexSession = new IndexSession<string>(model, indexStrategy, streamDispatcher, dataDirectory, collectionId, logger))
                {
                    foreach (var batch in documents.ReadDocuments(collectionId, fieldsOfInterest, skip, take).Batch(pageSize))
                    {
                        foreach (var document in batch)
                        {
                            foreach (var field in document.Fields)
                            {
                                indexSession.Put(document.Id, field.KeyId, (string)field.Value, label: false);
                            }

                            debugger.Step(indexSession);
                        }
                        indexSession.Commit();
                    }
                }
            }
        }

        private static void Print(string name, VectorNode tree)
        {
            var diagram = PathFinder.Visualize(tree);
            File.WriteAllText($@"c:\temp\{name}.txt", diagram);
        }
    }
}
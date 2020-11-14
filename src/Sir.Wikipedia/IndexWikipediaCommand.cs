﻿using Microsoft.Extensions.Logging;
using Sir.Search;
using Sir.VectorSpace;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace Sir.Wikipedia
{
    /// <summary>
    /// Download JSON search index dump here: 
    /// https://dumps.wikimedia.org/other/cirrussearch/current/enwiki-20201026-cirrussearch-content.json.gz
    /// </summary>
    public class IndexWikipediaCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["dataDirectory"];
            var fileName = args["fileName"];
            var collection = args["collection"];
            var skip = args.ContainsKey("skip") ? int.Parse(args["skip"]) : 0;
            var take = args.ContainsKey("take") ? int.Parse(args["take"]) : int.MaxValue;
            var sampleSize = args.ContainsKey("sampleSize") ? int.Parse(args["sampleSize"]) : 1000;
            var pageSize = args.ContainsKey("pageSize") ? int.Parse(args["pageSize"]) : 100000;

            var collectionId = collection.ToHash();
            var fieldsToStore = new HashSet<string> { "language", "wikibase_item", "title", "text" };
            var fieldsToIndex = new HashSet<string> { "language", "title", "text" };

            if (take == 0)
                take = int.MaxValue;

            var model = new BagOfCharsModel();
            var payload = WikipediaHelper.ReadWP(fileName, skip, take, fieldsToStore, fieldsToIndex);
            var debugger = new IndexDebugger(sampleSize);

            using (var sessionFactory = new SessionFactory(dataDirectory, logger))
            {
                sessionFactory.Truncate(collectionId);

                using (var stream = new IndexFileStreamProvider(collectionId, sessionFactory, logger: logger))
                using (var writeSession = sessionFactory.CreateWriteSession(collectionId))
                {
                    foreach (var page in payload.Batch(pageSize))
                    {
                        using (var indexSession = sessionFactory.CreateIndexSession(model))
                        {
                            foreach (var document in page)
                            {
                                var documentId = writeSession.Put(document);

                                Parallel.ForEach(document.IndexableFields, field =>
                                {
                                    indexSession.Put(documentId, field.Id, field.Value.ToString());
                                });
                                //foreach (var field in document.IndexableFields)
                                //{
                                //    indexSession.Put(documentId, field.Id, field.Value.ToString());
                                //}

                                var debugInfo = debugger.Step(indexSession);

                                if (debugInfo != null)
                                {
                                    logger.LogInformation(debugInfo);
                                }
                            }

                            stream.Write(indexSession.InMemoryIndex);

                            foreach (var column in indexSession.InMemoryIndex)
                            {
                                Print($"wikipedia.{column.Key}", column.Value);
                            }
                        }
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
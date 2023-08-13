using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.IO;
using Sir.Strings;

namespace Sir.Cmd
{
    public class AnalyzeDocumentCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["directory"];
            var collection = args["collection"];
            var documentId = long.Parse(args["documentId"]);
            var select = new HashSet<string>(args["select"].Split(new char[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries));
            var collectionId = collection.ToHash();
            var model = new BagOfCharsModel();
            var embedding = new SortedList<int, float>();
            using (var sessionFactory = new SessionFactory(logger))
            {
                var keys = new KeyRepository(dataDirectory, sessionFactory);
                using (var documents = new DocumentStreamSession(sessionFactory, keys, dataDirectory))
                using (var documentReader = new DocumentReader(dataDirectory, collectionId, sessionFactory))
                {
                    var doc = documents.ReadDocument((collectionId, documentId), select, documentReader);

                    foreach (var field in doc.Fields)
                    {
                        var tokens = model.CreateEmbedding(field.Value.ToString(), true, embedding);
                        var tree = new VectorNode();

                        foreach (var token in tokens)
                        {
                            tree.AddOrAppend(new VectorNode(vector: token), model);
                        }

                        Console.WriteLine(field.Name);
                        Console.WriteLine(PathFinder.Visualize(tree));
                        Console.WriteLine(string.Join('\n', tokens));
                    }
                }
            }
        }
    }
}
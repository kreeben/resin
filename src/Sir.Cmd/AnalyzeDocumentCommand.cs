using System;
using System.Collections.Generic;
using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.IO;
using Sir.KeyValue;
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

            using (var documentReader = new DocumentRegistryReader(dataDirectory, collectionId))
            {
                var doc = DocumentReader.Read(documentId, documentReader, select);

                foreach (var field in doc.Fields)
                {
                    var tokens = model.CreateEmbedding(field.Value.ToString(), true);
                    var tree = new VectorNode();

                    foreach (var token in tokens)
                    {
                        tree.AddOrAppend(new VectorNode(vector:token), model);
                    }

                    Console.WriteLine(field.Name);
                    Console.WriteLine(PathFinder.Visualize(tree));
                    Console.WriteLine(string.Join('\n', tokens));
                }
            }
        }
    }
}
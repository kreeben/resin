using Microsoft.Extensions.Logging;
using Sir.Documents;
using Sir.Strings;
using Sir.Wikipedia;
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Sir.Cmd
{
    public class BenchmarkCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            if (args.ContainsKey("tokenize"))
                RunTokenizeBenchmark(args, logger);
            else if (args.ContainsKey("index"))
                RunIndexBenchmark(args, logger);
        }

        /// <summary>
        /// E.g. benchmark --tokenize --file d:\enwiki-20211122-cirrussearch-content.json.gz --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --skip 0 --take 10000
        /// </summary>
        public void RunTokenizeBenchmark(IDictionary<string, string> args, ILogger logger)
        {
            const int numOfRuns = 10;
            int skip = int.Parse(args["skip"]);
            int take = int.Parse(args["take"]);
            var fileName = args["file"];
            var model = new BagOfCharsModel();
            var documents = new List<Document>(WikipediaHelper.Read(fileName, skip, take, new HashSet<string> { "text" }));
            var timer = Stopwatch.StartNew();

            for (int i = 0; i < numOfRuns; i++)
            {
                foreach (var document in documents)
                {
                    var embeddings = new List<ISerializableVector>(model.CreateEmbedding((string)document.Fields[0].Value, false));
                }
            }

            var totalTime = timer.Elapsed;
            var avgPerRun = totalTime.TotalMilliseconds / numOfRuns;
            var avgPerDoc = totalTime.TotalMilliseconds / documents.Count;
            Console.WriteLine($"Average {avgPerRun} ms/run. Average {avgPerDoc} ms/document.");
        }

        /// <summary>
        /// E.g. benchmark --index --file d:\enwiki-20211122-cirrussearch-content.json.gz --directory C:\projects\resin\src\Sir.HttpServer\AppData\database --skip 0 --take 10000 --collection wikipedia
        /// </summary>
        public void RunIndexBenchmark(IDictionary<string, string> args, ILogger logger)
        {
            const int numOfRuns = 1;

            var timer = Stopwatch.StartNew();

            for (int i = 0; i < numOfRuns; i++)
            {
                new DocumentDatabase<string>(args["directory"], "wikipedia".ToHash(), logger: logger).Truncate();
                new IndexWikipediaCommand().Run(args, logger);
            }

            var numOfDocs = int.Parse(args["take"]);
            var totalTime = timer.Elapsed;
            var avgPerRun = totalTime.TotalMilliseconds / numOfRuns;
            var avgPerDoc = totalTime.TotalMilliseconds / numOfDocs;
            Console.WriteLine($"Average {avgPerRun} ms/run. Average {avgPerDoc} ms/document.");
        }
    }
}
using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.KeyValue;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    /// <summary>
    /// lexicon --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --take 100 --debug true
    /// </summary>
    public class LexiconCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            var take = int.Parse(args["take"]);
            var sourcePath = args["source"];

            logger.LogInformation("Lexicon build starting.");
            logger.LogInformation("Arguments: dir={Dir}, source={Source}, take={Take}, truncate={Truncate}",
                dir.FullName,
                sourcePath,
                take,
                args.ContainsKey("truncate") ? args["truncate"] : "false");

            // Optional truncate
            if (args.ContainsKey("truncate") && args["truncate"] == "true")
            {
                logger.LogInformation("Truncating collections: wikipedia, wikipedia.composed (if present).");
                new StreamFactory(dir).Truncate("wikipedia".ToHash());
                logger.LogInformation("Truncate complete.");
            }

            logger.LogInformation("Initializing data source from {Source}.", sourcePath);
            var dataSource = new WikipediaCirrussearchDataSource(sourcePath).GetData(new HashSet<string> { "text" });

            // Materialize the first document’s values for a stable sample
            var sample = dataSource.First().values.Where(s => !string.IsNullOrWhiteSpace(s)).Take(take).ToArray();
            logger.LogInformation("Sample prepared: {Count} items.", sample.Length);

            using (var tx = new WriteSession(dir, "wikipedia".ToHash()))
            {
                logger.LogInformation("WriteSession opened at {Dir}.", dir.FullName);

                var analyzer = new StringAnalyzer();
                logger.LogInformation("StringAnalyzer configured: dims=512, identityAngle=0.9 (default).");
                logger.LogInformation("BuildLexicon started…");

                var sw = System.Diagnostics.Stopwatch.StartNew();
                analyzer.BuildLexicon(sample, tx, logger);
                sw.Stop();

                logger.LogInformation("BuildLexicon completed in {Elapsed}.", sw.Elapsed);
            }

            logger.LogInformation("Lexicon build finished.");
        }
    }
}

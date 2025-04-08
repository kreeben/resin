using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.KeyValue;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    /// <summary>
    /// lexicon --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --take 100
    /// </summary>
    public class LexiconCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            var take = int.Parse(args["take"]);
            var dataSource = new WikipediaCirrussearchDataSource(args["source"]).GetData(new HashSet<string> { "text" });
            if (args.ContainsKey("truncate") && args["truncate"] == "true")
                new StreamFactory(dir).Truncate("wikipedia".ToHash());
            using (var tx = new WriteTransaction(dir, "wikipedia".ToHash()))
            {
                new StringAnalyzer().BuildFirstOrderLexicon(dataSource.First().values.Take(take), tx, logger);
            }
        }
    }
}

using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.KeyValue;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    /// <summary>
    /// analyze --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text" --take 100
    /// </summary>
    public class ValidateCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            var dataSource = new WikipediaCirrussearchDataSource(args["source"]).GetData(new HashSet<string> { args["field"] });
            using (var tx = new WriteTransaction(dir, "wikipedia".ToHash()))
            using (var readSession = new ReadSession(tx))
            {
                new StringAnalyzer().Validate(dataSource.First().values.Take(int.Parse(args["take"])), readSession, logger);
            }

        }
    }
}

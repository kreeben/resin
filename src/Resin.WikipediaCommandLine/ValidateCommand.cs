using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    /// <summary>
    /// analyze --dir "c:\data" --source "d:\enwiki-20211122-cirrussearch-content.json.gz" --field "text"
    /// </summary>
    public class ValidateCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            var dataSource = new WikipediaCirrussearchDataSource(args["source"]).GetData(args["field"]);
            new StringAnalyzer(dir).Validate(dataSource.Take(100), logger);
        }
    }
}

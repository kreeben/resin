using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    public class ValidateCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(@"c:\data");
            var dataSource = new WikipediaCirrussearchDataSource(@"d:\enwiki-20211122-cirrussearch-content.json.gz").GetData("text");
            new StringAnalyzer(dir).Validate(dataSource.Take(100), logger);
        }
    }
}

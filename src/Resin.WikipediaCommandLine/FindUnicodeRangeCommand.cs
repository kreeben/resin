using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.DataSources;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    public class FindUnicodeRangeCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(@"c:\data");
            var dataSource = new WikipediaCirrussearchDataSource(@"d:\enwiki-20211122-cirrussearch-content.json.gz").GetData(new HashSet<string> { "text" });
            var unicodeRange = new StringAnalyzer().FindUnicodeRange(dataSource.First().values, logger);
            Console.WriteLine($"first code point: {unicodeRange.FirstCodePoint} length: {unicodeRange.Length}");
        }
    }
}

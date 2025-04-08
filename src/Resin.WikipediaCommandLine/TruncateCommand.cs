using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.KeyValue;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    public class TruncateCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var dir = new DirectoryInfo(args["dir"]);
            new StreamFactory(dir).Truncate(args["collectionId"].ToHash());
        }
    }
}

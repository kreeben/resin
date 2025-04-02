using Microsoft.Extensions.Logging;
using Resin.CommandLine;
using Resin.TextAnalysis;

namespace Resin.WikipediaCommandLine
{
    public class StringCompareCommand : ICommand
    {
        public void Run(IDictionary<string, string> args, ILogger logger)
        {
            var str1 = args["str1"];
            var str2 = args["str2"];

            var angle1 = new StringAnalyzer().CompareToUnitVector(str1);
            var angle2 = new StringAnalyzer().CompareToUnitVector(str2);

            Console.WriteLine($"{str1}: {angle1} and {str2}: {angle2}");
        }
    }
}

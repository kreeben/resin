using Microsoft.Extensions.Logging;

namespace Resin.CommandLine
{
    public interface ICommand
    {
        void Run(IDictionary<string, string> args, ILogger logger);
    }
}

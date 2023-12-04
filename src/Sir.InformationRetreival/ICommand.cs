using Microsoft.Extensions.Logging;
using System.Collections.Generic;

namespace Sir
{
    public interface ICommand
    {
        void Run(IDictionary<string, string> args, ILogger logger);
    }
}

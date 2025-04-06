using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Resin.CommandLine;

namespace Resin.WikipediaCommandLine
{
    internal class Program
    {
        static int Main(string[] args)
        {
            var command = args.Length == 0 ? "validate" : args[0].ToLower();
            var flags = ArgumentParser.ParseArgs(args);
            ICommand plugin = PluginReader.ResolvePlugin(command);
            ILoggerFactory loggerFactory;

            if (!flags.ContainsKey("debug") || flags["debug"] == "false")
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    builder
                        .AddFilter("Microsoft", LogLevel.Warning)
                        .AddFilter("System", LogLevel.Warning)
                        .AddFilter("Resin", LogLevel.Warning)
                        .AddConsole();
                });
            }
            else
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    builder
                        .AddFilter("Microsoft", LogLevel.Warning)
                        .AddFilter("System", LogLevel.Warning)
                        .AddFilter("Resin", LogLevel.Information)
                        .AddConsole();
                });
            }

            using (loggerFactory)
            {
                var logger = loggerFactory.CreateLogger("Resin");

                if (plugin != null)
                {
                    var time = Stopwatch.StartNew();
                    Console.WriteLine($"running command: {string.Join(" ", args)}");
                    try
                    {
                        plugin.Run(flags, logger);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, ex.Message);
                        throw;
                    }

                    time.Stop();
                    Console.WriteLine($"command finished in {time.Elapsed}: {string.Join(" ", args)} ");
                }
            }

            return 0;
        }
    }
}
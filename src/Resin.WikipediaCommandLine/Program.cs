using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Resin.CommandLine;

namespace Resin.WikipediaCommandLine
{
    internal class Program
    {
        static void Main(string[] args)
        {
            var command = args.Length == 0 ? "validate" : args[0].ToLower();
            var flags = ArgumentParser.ParseArgs(args);
            ICommand plugin = PluginReader.ResolvePlugin(command);
            ILoggerFactory loggerFactory;

            if (flags.ContainsKey("debug"))
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    builder
                        .AddFilter("Microsoft", LogLevel.Warning)
                        .AddFilter("System", LogLevel.Warning)
                        .AddFilter("Resin", LogLevel.Debug)
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
                    logger.LogInformation($"{command} command finished in {time.Elapsed}");
                }
            }

            Console.WriteLine("press any key to quit");

            Console.Read();
        }




    }
}
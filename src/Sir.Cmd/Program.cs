using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sir.Strings;

namespace Sir.Cmd
{
    class Program
    {
        static void Main(string[] args)
        {
            ILoggerFactory loggerFactory;

            var command = args[0].ToLower();
            var flags = ParseArgs(args);

            if (flags.ContainsKey("debug"))
            {
                loggerFactory = LoggerFactory.Create(builder =>
                {
                    builder
                        .AddFilter("Microsoft", LogLevel.Warning)
                        .AddFilter("System", LogLevel.Warning)
                        .AddFilter("Sir", LogLevel.Debug)
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
                        .AddFilter("Sir", LogLevel.Information)
                        .AddConsole();
                });
            }

            var logger = loggerFactory.CreateLogger("Sir");

            logger.LogInformation($"processing command: {string.Join(" ", args)}");

            var plugin = ResolvePlugin(command);
            var time = Stopwatch.StartNew();

            if (plugin != null)
            {
                try
                {
                    plugin.Run(flags, logger);
                }
                catch (Exception ex)
                {
                    logger.LogError(ex, ex.Message);
                }
            }
            else if ((command == "slice"))
            {
                Slice(flags);
            }
            else if (command == "truncate")
            {
                Truncate(flags["directory"], flags["collection"], logger);
            }
            else if (command == "truncate-index")
            {
                TruncateIndex(flags["directory"], flags["collection"], logger);
            }
            else if (command == "optimize")
            {
                Optimize(flags, logger);
            }
            else if (command == "rename")
            {
                Rename(flags["directory"], flags["collection"], flags["newCollection"], logger);
            }
            else
            {
                logger.LogInformation("unknown command: {0}", command);

                return;
            }

            logger.LogInformation($"executed {command} in {time.Elapsed}");
        }

        private static ICommand ResolvePlugin(string command)
        {
            var reader = new PluginReader(Directory.GetCurrentDirectory());
            var plugins = reader.Read<ICommand>("command");

            if (!plugins.ContainsKey(command))
                return null;

            return plugins[command];
        }

        private static IDictionary<string, string> ParseArgs(string[] args)
        {
            var dic = new Dictionary<string, string>();

            for (int i = 1; i < args.Length; i += 2)
            {
                var key = args[i].Replace("--", "");
                var value = args[i + 1];

                if (value.StartsWith("--"))
                {
                    dic.Add(key, "true");
                    i--;
                }
                else
                {
                    dic.Add(key, i == args.Length - 1 ? null : value);
                }
            }

            return dic;
        }

        private static void Optimize(IDictionary<string, string> args, ILogger logger)
        {
            var dataDirectory = args["directory"];
            var collection = args["collection"];
            var skip = int.Parse(args["skip"]);
            var take = int.Parse(args["take"]);
            var reportFrequency = int.Parse(args["reportFrequency"]);
            var pageSize = int.Parse(args["pageSize"]);
            var fields = new HashSet<string>(args["fields"].Split(','));
            var model = new BagOfCharsModel();

            using (var sessionFactory = new SessionFactory(logger))
            {
                sessionFactory.Optimize(
                    dataDirectory,
                    collection, 
                    fields,
                    model,
                    new LogStructuredIndexingStrategy(model),
                    skip,
                    take,
                    reportFrequency,
                    pageSize);
            }
        }

        private static void Slice(IDictionary<string, string> args)
        {
            var file = args["sourceFileName"];
            var slice = args["resultFileName"];
            var len = int.Parse(args["length"]);

            Span<byte> buf = new byte[len];

            using (var fs = File.OpenRead(file))
            using (var target = File.Create(slice))
            {
                fs.Read(buf);
                target.Write(buf);
            }
        }

        private static void Truncate(string dataDirectory, string collection, ILogger log)
        {
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.Truncate(dataDirectory, collectionId);
            }
        }

        private static void TruncateIndex(string dataDirectory, string collection, ILogger log)
        {
            var collectionId = collection.ToHash();

            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.TruncateIndex(dataDirectory, collectionId);
            }
        }

        private static void Rename(string dataDirectory, string currentCollectionName, string newCollectionName, ILogger log)
        {
            using (var sessionFactory = new SessionFactory(log))
            {
                sessionFactory.Rename(dataDirectory, currentCollectionName.ToHash(), newCollectionName.ToHash());
            }
        }

        private static void Serialize(IEnumerable<object> docs, Stream stream)
        {
            using (StreamWriter writer = new StreamWriter(stream))
            using (JsonTextWriter jsonWriter = new JsonTextWriter(writer))
            {
                JsonSerializer ser = new JsonSerializer();
                ser.Serialize(jsonWriter, docs);
                jsonWriter.Flush();
            }
        }
    }
}
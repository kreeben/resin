using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Sir.Strings;
using System;
using System.IO;

namespace Sir.HttpServer
{
    public static class ServiceConfiguration
    {
        public static IServiceProvider Configure(IServiceCollection services)
        {
            var assemblyPath = Directory.GetCurrentDirectory();
            var config = new KeyValueConfiguration(Path.Combine(assemblyPath, "sir.ini"));

            services.Add(new ServiceDescriptor(typeof(IConfigurationProvider), config));

            var loggerFactory = services.BuildServiceProvider().GetService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger("Sir");
            var model = new BagOfCharsModel();
            var directory = config.Get("data_dir");
            var defaultCollection = config.Get("default_collection");
            var qp = new QueryParser<string>(new KeyValue.KeyValueReader(directory, defaultCollection.ToHash()), model, logger: logger);
            var httpParser = new HttpQueryParser(qp);

            services.AddSingleton(typeof(IModel<string>), model);
            services.AddSingleton(typeof(QueryParser<string>), qp);
            services.AddSingleton(typeof(HttpQueryParser), httpParser);
            services.AddSingleton(typeof(IHttpWriter), new HttpWriter(config));
            services.AddSingleton(typeof(IHttpReader), new HttpReader(
                httpParser,
                config,
                loggerFactory.CreateLogger<HttpReader>()));

            return services.BuildServiceProvider();
        }
    }
}

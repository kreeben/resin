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

            services.AddTransient<IModel<string>, BagOfCharsModel>();
            services.AddTransient<IModel, BagOfCharsModel>();
            services.AddTransient<IIndexReadWriteStrategy, LogStructuredIndexingStrategy>();
            services.AddTransient<IConfigurationProvider>((x) => { return new KeyValueConfiguration(Path.Combine(assemblyPath, "sir.ini")); } );
            services.AddTransient<HttpWriter>();
            services.AddTransient<HttpReader>();

            return services.BuildServiceProvider();
        }
    }
}

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Sir.IO;
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

            var directory = config.Get("data_dir");
            var loggerFactory = services.BuildServiceProvider().GetService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger("Sir");
            var model = new BagOfCharsModel();
            var sessionFactory = new SessionFactory(logger);
            var keyRepository = new KeyRepository(directory, sessionFactory);
            var qp = new QueryParser<string>(directory, keyRepository, model, logger: logger);
            var httpParser = new HttpQueryParser(qp);

            services.AddSingleton(typeof(IModel<string>), model);
            services.AddSingleton(typeof(ISessionFactory), sessionFactory);
            services.AddSingleton(typeof(SessionFactory), sessionFactory);
            services.AddSingleton(typeof(QueryParser<string>), qp);
            services.AddSingleton(typeof(HttpQueryParser), httpParser);
            services.AddSingleton(typeof(WriteClient), new WriteClient(sessionFactory, config));
            services.AddSingleton(typeof(SearchClient), new SearchClient(
                sessionFactory,
                httpParser,
                config,
                loggerFactory.CreateLogger<SearchClient>()));

            return services.BuildServiceProvider();
        }
    }
}

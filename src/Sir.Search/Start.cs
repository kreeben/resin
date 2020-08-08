using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Sir.Search
{
    /// <summary>
    /// Initialize app.
    /// </summary>
    public class Start : IPluginStart
    {
        public void OnApplicationStartup(
            IServiceCollection services, ServiceProvider serviceProvider, IConfigurationProvider config)
        {
            var loggerFactory = serviceProvider.GetService<ILoggerFactory>();
            var model = new BocModel();
            var sessionFactory = new SessionFactory(
                config, 
                model, 
                loggerFactory.CreateLogger<SessionFactory>());

            var qp = new QueryParser(sessionFactory, model, loggerFactory.CreateLogger<QueryParser>());

            var httpParser = new HttpQueryParser(qp);

            services.AddSingleton(typeof(IStringModel), model);
            services.AddSingleton(typeof(ISessionFactory), sessionFactory);
            services.AddSingleton(typeof(SessionFactory), sessionFactory);
            services.AddSingleton(typeof(QueryParser), qp);
            services.AddSingleton(typeof(HttpQueryParser), httpParser);
            services.AddSingleton(typeof(IHttpWriter), new HttpWriter(sessionFactory));
            services.AddSingleton(typeof(IHttpReader), new HttpReader(
                sessionFactory, 
                httpParser, 
                config, 
                loggerFactory.CreateLogger<HttpReader>()));
        }
    }
}
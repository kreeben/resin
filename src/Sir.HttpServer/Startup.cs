using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using System;
using System.IO;

namespace Sir.HttpServer
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        public IServiceProvider ServiceProvider { get; private set; }

        public void ConfigureServices(IServiceCollection services)
        {
            ServiceProvider = ServiceConfiguration.Configure(services);

            services.AddMvc(options =>
            {
                options.RespectBrowserAcceptHeader = true;
                options.EnableEndpointRouting = false;
            });

            services.AddControllersWithViews().AddRazorRuntimeCompilation();

            services.Configure<IISOptions>(options =>
            {
                options.ForwardClientCertificate = false;
            });

            var dataDir = ServiceProvider.GetService<IConfigurationProvider>().Get("data_dir");

            if (!Directory.Exists(dataDir))
            {
                Directory.CreateDirectory(dataDir);
            }
        }

        public void Configure(
            IApplicationBuilder app, 
            IHostApplicationLifetime applicationLifetime, 
            IWebHostEnvironment env)
        {
            applicationLifetime.ApplicationStopping.Register(OnShutdown);

            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseMvc(routes =>
            {
                routes.MapRoute("default", "{controller}/{action}", new { controller = "Home", action = "Index" });
            });

            app.UseStaticFiles();
        }

        private void OnShutdown()
        {
        }
    }
}

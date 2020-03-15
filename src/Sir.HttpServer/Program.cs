using System;
using System.IO;
using System.Text;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Sir.HttpServer
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var host = CreateHostBuilder(args).Build();
            host.Run();
        }

        public static IHostBuilder CreateHostBuilder(string[] args) =>
            Host.CreateDefaultBuilder(args)
            .ConfigureLogging(logging =>
            {
                logging.ClearProviders();
                logging.AddDebug();

                //if (!Directory.Exists("AppData"))
                //    Directory.CreateDirectory("AppData");
                //logging.AddFile("AppData/log-{Date}.txt");
            })
            .ConfigureWebHostDefaults(webBuilder =>
            {
                webBuilder.ConfigureKestrel(options =>
                {
                    options.Limits.MinRequestBodyDataRate =
                            new MinDataRate(bytesPerSecond: 100, gracePeriod: TimeSpan.FromSeconds(10));
                    options.Limits.MinResponseDataRate =
                        new MinDataRate(bytesPerSecond: 100, gracePeriod: TimeSpan.FromSeconds(10));
                })
                .UseContentRoot(Directory.GetCurrentDirectory())
                .UseIISIntegration()
                    .CaptureStartupErrors(true)
                    .UseSetting("detailedErrors", "true")
                .UseStartup<Startup>();
            });
    }

    public static class WebExtensions
    {
        public static string ToStringExcept(this IQueryCollection collection, string key)
        {
            var result = new StringBuilder();
            var index = 0;

            foreach (var field in collection)
            {
                if (field.Key != key)
                {
                    foreach (var value in field.Value)
                    {
                        if (index++ > 0)
                        {
                            result.Append("&");
                        }

                        result.Append($"{field.Key}={value}");
                    }
                }
            }

            return result.ToString();
        }
    }
}

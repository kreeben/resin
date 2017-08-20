﻿using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using System;

namespace Resin.DocumentSearchServer
{
    public class Program
    {
        public static Searcher Searcher { get; private set; }

        public static void Main(string[] args)
        {
            string dataDirectory = "c:\\temp\\resin_data\\pg";
            string postingsServerHostName = null;
            int port = 0;

            if (Array.IndexOf(args, "--ps") > -1)
            {
                postingsServerHostName = args[Array.IndexOf(args, "--ps") + 1];
                port = int.Parse(args[Array.IndexOf(args, "--port") + 1]);
            }

            if (Array.IndexOf(args, "--dir") > -1)
                dataDirectory = args[Array.IndexOf(args, "--dir") + 1];

            var bin = Directory.GetCurrentDirectory();
            Directory.SetCurrentDirectory(dataDirectory);

            if (postingsServerHostName == null)
            {
                Searcher = new Searcher(dataDirectory);
            }
            else
            {
                Searcher = new Searcher(dataDirectory, sessionFactory: new NetworkFullTextReadSessionFactory(
                postingsServerHostName, port, dataDirectory));
            }

            var host = new WebHostBuilder()
                .UseKestrel()
                .UseContentRoot(bin)
                .UseIISIntegration()
                .UseStartup<Startup>()
                .Build();

            host.Run();

            Searcher.Dispose();
        }
    }
}

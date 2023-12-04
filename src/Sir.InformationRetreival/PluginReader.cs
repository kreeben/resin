using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Loader;

namespace Sir
{
    public class PluginReader
    {
        private readonly string _directory;

        public PluginReader(string directory)
        {
            _directory = directory;
        }

        public IDictionary<string, T> Read<T>(string commonTypeName)
        {
            var plugins = new Dictionary<string, T>();
            var files = Directory.GetFiles(_directory, "*.dll");

            foreach (var assembly in files.Select(file => AssemblyLoadContext.Default.LoadFromAssemblyPath(file)))
            {
                foreach (var type in assembly.GetTypes())
                {
                    // search for concrete types
                    if (!type.IsInterface)
                    {
                        var interfaces = type.GetInterfaces();

                        if (interfaces.Contains(typeof(T)))
                        {
                            var name = type.Name.Replace(commonTypeName, "", StringComparison.OrdinalIgnoreCase).ToLower();

                            plugins.Add(name, (T)Activator.CreateInstance(type));
                        }
                    }
                }
            }

            return plugins;
        }
    }
}

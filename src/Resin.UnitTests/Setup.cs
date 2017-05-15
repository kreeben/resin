using Microsoft.VisualStudio.TestTools.UnitTesting;
using Resin.IO;
using Resin.Sys;
using System;
using System.Collections.Generic;
using System.IO;

namespace Tests
{
    [TestClass()]
    public class Setup
    {
        private const string root = @"c:\temp\resin_tests\";

        protected static string CreateDir()
        {
            var dir = root + Guid.NewGuid().ToString();
            Directory.CreateDirectory(dir);
            return dir;
        }

        [AssemblyInitialize()]
        public static void AssemblyInit(TestContext context)
        {
            foreach(var dir in Directory.GetDirectories(root))
            {
                Directory.Delete(dir, true);
            }
        }
    }

    public static class DocumentHelper
    {
        public static IEnumerable<Document> ToDocuments(this IEnumerable<dynamic> dynamicDocuments)
        {
            foreach (var dyn in dynamicDocuments)
            {
                var fields = new List<Field>();

                foreach (var field in Util.ToDictionary(dyn))
                {
                    fields.Add(new Field(field.Key, field.Value));
                }

                yield return new Document(fields);
            }
        }
    }
}